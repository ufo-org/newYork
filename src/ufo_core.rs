use std::marker::PhantomData;
use std::ops::Deref;
use std::result::Result;
use std::sync::{Arc, LockResult, Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::{cmp::min, vec::Vec};
use std::{
    collections::{HashMap, VecDeque},
    sync::MutexGuard,
};

use log::{debug, info, trace};

use rayon::iter::{IntoParallelIterator, ParallelIterator};

use super::errors::*;
use super::mmap_wrapers::*;
use super::ufo_objects::*;

pub(crate) trait Locker<'a> {
    type Guard;

    fn lock(&'a self) -> Self::Guard;
}

pub(crate) struct WriteLocker<'a>(pub ChunkArcLock, pub PhantomData<&'a ChunkArcLock>);

impl<'a> Locker<'a> for WriteLocker<'a> {
    type Guard = RwLockWriteGuard<'a, UfoChunk>;

    fn lock(&'a self) -> Self::Guard {
        self.0.write().unwrap()
    }
}

pub(crate) struct ReadLocker<'a>(pub ChunkArcLock, pub PhantomData<&'a ChunkArcLock>);

impl<'a> Locker<'a> for ReadLocker<'a> {
    type Guard = RwLockReadGuard<'a, UfoChunk>;

    fn lock(&'a self) -> Self::Guard {
        self.0.read().unwrap()
    }
}

pub(crate) struct Locked<'a, T> {
    pub guard: T,
    pub _p: PhantomData<&'a T>,
}

struct UfoChunks {
    loaded_chunks: VecDeque<Arc<RwLock<UfoChunk>>>,
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

    fn add(&mut self, chunk: ChunkArcLock, size: usize) {
        self.used_memory += size;
        self.loaded_chunks.push_back(chunk);
    }

    fn drop_ufo_chunks(&mut self, ufo_id: UfoId) {
        let chunks = &mut self.loaded_chunks;
        chunks
            .iter_mut()
            .map(|c| c.write().expect("chunk lock broken"))
            .filter(|c| c.ufo_id() == ufo_id)
            .for_each(|mut c| c.mark_freed());
        self.used_memory = chunks
            .iter()
            .map(|c| c.read().expect("chunk lock broken"))
            .map(|c| c.size())
            .sum();
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
                    let size = chunk.read().unwrap().size(); // chunk.free_and_writeback_dirty()?;
                    will_free_bytes += size;
                    to_free.push(chunk);
                    // self.used_memory -= size;
                }
            }
        }

        let freed_memory = to_free
            .into_par_iter()
            .map(|c| c.write().unwrap().free_and_writeback_dirty())
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

            debug!(target: "ufo_core", "mmapped {:#x} - {:#x}", mmap_base, mmap_base + true_size);

            let writeback = UfoFileWriteback::new(id, &config, this)?;

            //Pre-zero the header, that isn't part of our populate duties
            if config.header_size_with_padding > 0 {
                unsafe { mmap_ptr.write_bytes(0, config.header_size_with_padding) };
            }

            // let header_offset = config.header_size_with_padding - config.header_size;
            // let body_offset = config.header_size_with_padding;
            // let chunk_ct = config
            //     .element_ct()
            //     .div_ceil(config.elements_loaded_at_once());
            let ufo = UfoObject {
                id,
                core: Arc::downgrade(this),
                config,
                mmap,
                writeback_util: writeback,
                loaded_chunks: RwLock::new(HashMap::new()),
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

    pub(crate) fn populate_for_read<C>(
        &self,
        ufo_arc: &WrappedUfoObject,
        ufo: &UfoObject,
        fault_offset: UfoOffset,
        lock_consumer: C,
    ) -> Result<(), UfoErr>
    where
        C: FnOnce(&RwLockReadGuard<UfoChunk>),
    {
        let table_lock = ufo.loaded_chunks.read()?;
        if let Some(existing_chunk) = table_lock.get(&UfoChunkIdx(fault_offset.chunk_number())) {
            let locked_chunk = existing_chunk.read().unwrap();
            assert!(locked_chunk.size() > 0, "freed chunk witnessed");
            lock_consumer(&locked_chunk);
            return Ok(());
        }

        std::mem::drop(table_lock);

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

        let new_chunk = Arc::new(RwLock::new(UfoChunk::new(
            &ufo_arc,
            &ufo,
            populate_offset,
            populate_size,
        )));
        // grab the lock AFTER releasing the state lock
        let locked_new_chunk = new_chunk.read().unwrap();

        let config = &ufo.config;
        let offset = locked_new_chunk.offset();

        let mut loaded_chunks = ufo.loaded_chunks.write()?;
        let insert_result =
            loaded_chunks.try_insert(UfoChunkIdx(offset.as_index_floor()), Arc::clone(&new_chunk));
        if let Err(occ) = insert_result {
            // Someone was racing with us to load it and won
            let existing_chunk = occ.entry.get();
            let locked_existing_chunk = existing_chunk.read().unwrap();
            assert!(locked_existing_chunk.size() > 0, "freed chunk witnessed");
            return Ok(lock_consumer(&locked_existing_chunk));
        }

        std::mem::drop(loaded_chunks); // drop the table lock early, but while we hold the chunk lock

        let write_ptr = unsafe { ufo.body_ptr().add(offset.body_offset()).cast() };
        let readback = ufo.writeback_util.try_readback(&offset);
        match readback {
            Some(read_data) => {
                let write_slice =
                    unsafe { std::slice::from_raw_parts_mut(write_ptr, locked_new_chunk.size()) };
                assert!(write_slice.len() == read_data.len());
                write_slice.copy_from_slice(read_data);
            }
            None => {
                trace!(target: "ufo_core", "calculate");
                (config.populate)(start, pop_end, write_ptr)?;
            }
        }
        trace!(target: "ufo_core", "data ready");

        let mut state = self.get_locked_state().unwrap();

        state
            .loaded_chunks
            .add(Arc::clone(&new_chunk), locked_new_chunk.size()); //TODO: need to hand out Arcs
        trace!(target: "ufo_core", "chunk saved");

        Ok(lock_consumer(&locked_new_chunk))
    }

    pub(crate) fn populate_for_write<C>(
        &self,
        ufo_arc: &WrappedUfoObject,
        ufo: &UfoObject,
        fault_offset: UfoOffset,
        lock_consumer: C,
    ) -> Result<(), UfoErr>
    where
        C: FnOnce(&mut RwLockWriteGuard<UfoChunk>),
    {
        let table_lock = ufo.loaded_chunks.read()?;
        if let Some(existing_chunk) = table_lock.get(&UfoChunkIdx(fault_offset.chunk_number())) {
            let mut locked_chunk = existing_chunk.write().unwrap();
            assert!(locked_chunk.size() > 0, "freed chunk witnessed");
            lock_consumer(&mut locked_chunk);
            return Ok(());
        }

        std::mem::drop(table_lock);

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

        let new_chunk = Arc::new(RwLock::new(UfoChunk::new(
            &ufo_arc,
            &ufo,
            populate_offset,
            populate_size,
        )));
        // grab the lock AFTER releasing the state lock
        let mut locked_new_chunk = new_chunk.write().unwrap();

        let config = &ufo.config;
        let offset = locked_new_chunk.offset();

        let mut loaded_chunks = ufo.loaded_chunks.write()?;
        let insert_result =
            loaded_chunks.try_insert(UfoChunkIdx(offset.as_index_floor()), Arc::clone(&new_chunk));
        if let Err(occ) = insert_result {
            // Someone was racing with us to load it and won
            let existing_chunk = occ.entry.get();
            let mut locked_existing_chunk = existing_chunk.write().unwrap();
            assert!(locked_existing_chunk.size() > 0, "freed chunk witnessed");
            return Ok(lock_consumer(&mut locked_existing_chunk));
        }

        std::mem::drop(loaded_chunks); // drop the table lock early, but while we hold the chunk lock

        let write_ptr = unsafe { ufo.body_ptr().add(offset.body_offset()).cast() };
        let readback = ufo.writeback_util.try_readback(&offset);
        match readback {
            Some(read_data) => {
                let write_slice =
                    unsafe { std::slice::from_raw_parts_mut(write_ptr, locked_new_chunk.size()) };
                assert!(write_slice.len() == read_data.len());
                write_slice.copy_from_slice(read_data);
            }
            None => {
                trace!(target: "ufo_core", "calculate");
                (config.populate)(start, pop_end, write_ptr)?;
            }
        }
        trace!(target: "ufo_core", "data ready");

        let mut state = self.get_locked_state().unwrap();

        state
            .loaded_chunks
            .add(Arc::clone(&new_chunk), locked_new_chunk.size()); //TODO: need to hand out Arcs
        trace!(target: "ufo_core", "chunk saved");

        Ok(lock_consumer(&mut locked_new_chunk))
    }

    pub(crate) fn reset_impl(&self, ufo: &WrappedUfoObject) -> Result<(), UfoErr> {
        let state = &mut *self.get_locked_state()?;
        let mut ufo = ufo.write()?;

        debug!(target: "ufo_core", "resetting {:?}", ufo.id);
        ufo.reset_internal()?;

        state.loaded_chunks.drop_ufo_chunks(ufo.id);

        // this.assert_segment_map();

        Ok(())
    }

    pub(crate) fn free(&self, ufo: &WrappedUfoObject) -> Result<(), UfoErr> {
        let state = &mut *self.get_locked_state()?;
        let ufo = ufo.write()?;

        debug!(target: "ufo_core", "freeing {:?} @ {:?}", ufo.id, ufo.mmap.as_ptr());
        state.loaded_chunks.drop_ufo_chunks(ufo.id);

        Ok(())
    }

    pub fn shutdown(&self) {
        info!(target: "ufo_core", "shutting down");
        let ufos: Vec<WrappedUfoObject> = {
            let state = &mut *self.get_locked_state().expect("err on shutdown");
            state.objects_by_id.values().cloned().collect()
        };

        ufos.iter().for_each(|k| self.free(k).expect("err on free"));
    }
}
