use std::result::Result;
use std::sync::{Arc, Mutex, RwLock};
use std::{cmp::min, ops::Range, vec::Vec};
use std::{
    collections::{HashMap, VecDeque},
    sync::MutexGuard,
};

use log::{debug, info, trace};

use crossbeam::sync::WaitGroup;
use rayon::iter::{IntoParallelIterator, ParallelIterator};

use crate::bitmap::Bitmap;
use crate::once_await::OnceFulfiller;

use super::errors::*;
use super::mmap_wrapers::*;
use super::ufo_objects::*;
use super::write_buffer::*;

pub enum UfoInstanceMsg {
    Shutdown(WaitGroup),
    Allocate(promissory::Fulfiller<WrappedUfoObject>, UfoObjectConfig),
    Reset(WaitGroup, UfoId),
    Free(WaitGroup, UfoId),
}

struct UfoChunks {
    loaded_chunks: VecDeque<UfoChunk>,
    used_memory: usize,
    config: Arc<UfoCoreConfig>,
}

impl UfoChunks {
    fn new(config: Arc<UfoCoreConfig>) -> UfoChunks {
        UfoChunks {
            loaded_chunks: VecDeque::new(),
            used_memory: 0,
            config,
        }
    }

    fn add(&mut self, chunk: UfoChunk) {
        self.used_memory += chunk.size();
        self.loaded_chunks.push_back(chunk);
    }

    fn drop_ufo_chunks(&mut self, ufo_id: UfoId) {
        let chunks = &mut self.loaded_chunks;
        chunks
            .iter_mut()
            .filter(|c| c.ufo_id() == ufo_id)
            .for_each(UfoChunk::mark_freed);
        self.used_memory = chunks.iter().map(UfoChunk::size).sum();
    }

    fn free_until_low_water_mark(&mut self) -> anyhow::Result<usize> {
        debug!(target: "ufo_core", "Freeing memory");
        let low_water_mark = self.config.low_watermark;

        let mut to_free = Vec::new();
        let mut will_free_bytes = 0;

        while self.used_memory - will_free_bytes > low_water_mark {
            match self.loaded_chunks.pop_front() {
                None => anyhow::bail!("nothing to free"),
                Some(chunk) => {
                    let size = chunk.size(); // chunk.free_and_writeback_dirty()?;
                    will_free_bytes += size;
                    to_free.push(chunk);
                    // self.used_memory -= size;
                }
            }
        }

        let freed_memory = to_free
            .into_par_iter()
            .map_init(ChunkFreer::new, |f, mut c| f.free_chunk(&mut c))
            .reduce(|| Ok(0), |a, b| Ok(a? + b?))?;

        debug!(target: "ufo_core", "Done freeing memory");

        self.used_memory -= freed_memory;
        assert!(self.used_memory <= low_water_mark);

        Ok(self.used_memory)
    }
}

pub struct UfoCoreConfig {
    pub writeback_temp_path: String,
    pub high_watermark: usize,
    pub low_watermark: usize,
}

pub type WrappedUfoObject = Arc<RwLock<UfoObject>>;

pub struct UfoCoreState {
    object_id_gen: UfoIdGen,
    objects_by_id: HashMap<UfoId, WrappedUfoObject>,
    loaded_chunks: UfoChunks,
}

pub struct UfoCore {
    pub config: Arc<UfoCoreConfig>,
    state: Mutex<UfoCoreState>,
}

impl UfoCore {
    pub fn new(config: UfoCoreConfig) -> Result<Arc<UfoCore>, std::io::Error> {
        let config = Arc::new(config);
        // We want zero capacity so that when we shut down there isn't a chance of any messages being lost
        // TODO CMYK 2021.03.04: find a way to close the channel but still clear the queue

        let state = Mutex::new(UfoCoreState {
            object_id_gen: UfoIdGen::new(),

            loaded_chunks: UfoChunks::new(Arc::clone(&config)),
            objects_by_id: HashMap::new(),
        });

        let core = Arc::new(UfoCore { config, state });

        Ok(core)
    }

    fn get_locked_state(&self) -> Result<MutexGuard<UfoCoreState>, UfoAllocateErr> {
        Ok(self.state.lock()?)
    }

    pub fn allocate_ufo(
        this: &Arc<Self>,
        config: UfoObjectConfig,
    ) -> Result<WrappedUfoObject, UfoAllocateErr> {
        info!(target: "ufo_object", "new Ufo {{
            header_size: {},
            stride: {},
            header_size_with_padding: {},
            true_size: {},

            elements_loaded_at_once: {},
            element_ct: {},
         }}",
            config.header_size,
            config.stride,

            config.header_size_with_padding,
            config.true_size,

            config.elements_loaded_at_once,
            config.element_ct,
        );

        let ufo = {
            let state = &mut *this.get_locked_state()?;

            let id_map = &state.objects_by_id;
            let id_gen = &mut state.object_id_gen;

            let id = id_gen.next(|k| {
                trace!(target: "ufo_core", "testing id {:?}", k);
                !k.is_sentinel() && !id_map.contains_key(k)
            });

            debug!(target: "ufo_core", "allocate {:?}: {} elements with stride {} [pad|header⋮body] [{}|{}⋮{}]",
                id,
                config.element_ct,
                config.stride,
                config.header_size_with_padding - config.header_size,
                config.header_size,
                config.stride * config.element_ct,
            );

            let mmap = BaseMmap::new(
                config.true_size,
                &[MemoryProtectionFlag::Read, MemoryProtectionFlag::Write],
                &[MmapFlag::Anonymous, MmapFlag::Private, MmapFlag::NoReserve],
                None,
            )
            .expect("Mmap Error");

            let mmap_ptr = mmap.as_ptr();
            let true_size = config.true_size;
            let mmap_base = mmap_ptr as usize;
            let segment = Range {
                start: mmap_base,
                end: mmap_base + true_size,
            };

            debug!(target: "ufo_core", "mmapped {:#x} - {:#x}", mmap_base, mmap_base + true_size);

            let writeback = UfoFileWriteback::new(id, &config, this)?;

            //Pre-zero the header, that isn't part of our populate duties
            if config.header_size_with_padding > 0 {
                unsafe { mmap_ptr.write_bytes(0, config.header_size_with_padding) };
            }

            // let header_offset = config.header_size_with_padding - config.header_size;
            // let body_offset = config.header_size_with_padding;
            let chunk_ct = config
                .element_ct()
                .div_ceil(config.elements_loaded_at_once());
            let ufo = UfoObject {
                id,
                core: Arc::downgrade(this),
                config,
                mmap,
                writeback_util: writeback,
                loaded_chunks: RwLock::new(Bitmap::new(chunk_ct)),
            };

            let ufo = Arc::new(RwLock::new(ufo));

            state.objects_by_id.insert(id, ufo.clone());
            Ok(ufo)
        };

        ufo
    }

    fn ensure_capcity(config: &UfoCoreConfig, state: &mut UfoCoreState, to_load: usize) {
        assert!(to_load + config.low_watermark < config.high_watermark);
        if to_load + state.loaded_chunks.used_memory > config.high_watermark {
            state.loaded_chunks.free_until_low_water_mark().unwrap();
        }
    }

    pub fn get_ufo_by_id(&self, id: UfoId) -> Result<WrappedUfoObject, UfoErr> {
        self.get_locked_state()
            .map_err(|e| UfoErr::CoreBroken(format!("{:?}", e)))?
            .objects_by_id
            .get(&id)
            .cloned()
            .map(Ok)
            .unwrap_or_else(|| Err(UfoErr::UfoNotFound))
    }

    pub(crate) fn populate_impl(
        &self,
        ufo_arc: Arc<RwLock<UfoObject>>,
        fault_offset: UfoOffset,
    ) -> Result<(), UfoErr> {
        let ufo = ufo_arc.read().unwrap();
        if ufo
            .loaded_chunks
            .read()?
            .test(fault_offset.chunk_number())?
        {
            return Ok(());
        }

        let mut state = self.get_locked_state().unwrap();

        let config = &ufo.config;
        let load_size = config.elements_loaded_at_once * config.stride;
        let populate_offset = fault_offset.down_to_nearest_n_relative_to_header(load_size);

        let start = populate_offset.as_index_floor();
        let end = start + config.elements_loaded_at_once;
        let pop_end = min(end, config.element_ct);

        let populate_size = min(
            load_size,
            config.true_size - populate_offset.absolute_offset(),
        );

        debug!(target: "ufo_core", "fault at {}, populate {} bytes at {:#x}",
            start, (pop_end-start) * config.stride, populate_offset.as_ptr_int());

        // Before we perform the load ensure that there is capacity
        UfoCore::ensure_capcity(&self.config, &mut *state, load_size);

        // drop the lock before loading so that UFOs can be recursive
        Mutex::unlock(state);

        let chunk = UfoChunk::new(&ufo_arc, &ufo, populate_offset, populate_size);
        let config = &ufo.config;
        trace!("spin locking {:?}.{}", ufo.id, chunk.offset());
        let chunk_lock = ufo // grab the lock AFTER releasing the core lock
            .writeback_util
            .chunk_locks
            .spinlock(chunk.offset().chunk_number())
            .map_err(|_| UfoErr::UfoLockBroken)?;

        let chunk_loaded = ufo
            .loaded_chunks
            .write()?
            .set(chunk.offset().as_index_floor(), true)?;
        if chunk_loaded {
            // Someone was racing with us to load it
            return Ok(());
        }

        let mut buffer = UfoWriteBuffer::new();
        let raw_data = ufo
            .writeback_util
            .try_readback(&chunk.offset())
            .map(Ok::<&[u8], UfoErr>)
            .unwrap_or_else(|| {
                trace!(target: "ufo_core", "calculate");
                unsafe {
                    buffer.ensure_capcity(load_size);
                    (config.populate)(start, pop_end, buffer.ptr)?;
                    Ok(&buffer.slice()[0..load_size])
                }
            })?;
        assert!(raw_data.len() == load_size);
        trace!(target: "ufo_core", "data ready");

        let chunk_slice = unsafe {
            std::slice::from_raw_parts_mut(
                ufo.body_ptr().add(chunk.offset().body_offset()).cast(),
                chunk.size(),
            )
        };
        chunk_slice.copy_from_slice(raw_data);
        trace!(target: "ufo_core", "populated");

        let hash_fulfiller = chunk.hash_fulfiller();

        let mut state = self.get_locked_state().unwrap();

        trace!("unlock populate {:?}.{}", ufo.id, chunk.offset());
        chunk_lock.unlock(); // release only after we acquired the state lock

        state.loaded_chunks.add(chunk);
        trace!(target: "ufo_core", "chunk saved");

        // release the lock before calculating the hash so other workers can proceed
        Mutex::unlock(state);

        if !config.should_try_writeback() {
            hash_fulfiller.try_init(None);
        } else {
            // Make sure to take a slice of the raw data. the kernel operates in page sized chunks but the UFO ends where it ends
            let calculated_hash = hash_function(&raw_data[0..populate_size]);
            hash_fulfiller.try_init(Some(calculated_hash));
        }

        Ok(())
    }

    pub(crate) fn reset_impl(&self, ufo_id: UfoId) -> Result<(), UfoErr> {
        {
            let state = &mut *self.get_locked_state()?;

            let ufo = &mut *(state
                .objects_by_id
                .get(&ufo_id)
                .map(Ok)
                .unwrap_or_else(|| Err(UfoErr::UfoNotFound))?
                .write()?);

            debug!(target: "ufo_core", "resetting {:?}", ufo.id);

            ufo.reset_internal()?;

            state.loaded_chunks.drop_ufo_chunks(ufo_id);
        }

        // this.assert_segment_map();

        Ok(())
    }

    pub(crate) fn free(&self, ufo_id: UfoId) -> anyhow::Result<()> {
        let state = &mut *self.get_locked_state()?;
        let ufo = state
            .objects_by_id
            .remove(&ufo_id)
            .map(Ok)
            .unwrap_or_else(|| Err(anyhow::anyhow!("No such Ufo")))?;
        let ufo = ufo
            .write()
            .map_err(|_| anyhow::anyhow!("Broken Ufo Lock"))?;

        debug!(target: "ufo_core", "freeing {:?} @ {:?}", ufo.id, ufo.mmap.as_ptr());
        state.loaded_chunks.drop_ufo_chunks(ufo_id);

        Ok(())
    }

    pub fn shutdown(&self) {
        info!(target: "ufo_core", "shutting down");
        let keys: Vec<UfoId> = {
            let state = &mut *self.get_locked_state().expect("err on shutdown");
            state.objects_by_id.keys().cloned().collect()
        };

        keys.iter()
            .for_each(|k| self.free(*k).expect("err on free"));
    }
}
