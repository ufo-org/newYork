use libc::c_void;
use libc::memcpy;

use crate::ufo_core::*;
use crate::ufo_objects::*;

use super::prototype::BoroughParameters;
use super::prototype::BoroughPopulateCallout;
use super::prototype::BoroughPopulateData;

#[repr(C)]
pub struct Borough {
    ptr: *mut c_void,
}

opaque_c_type!(Borough, WrappedUfoObject);

type AssocData = Box<(BoroughPopulateCallout, BoroughPopulateData)>;

impl Borough {
    #[no_mangle]
    pub extern "C" fn borough_free(self) {
        std::panic::catch_unwind(|| {
            if let Some(ufo) = self.deref() {
                let ufo_lock = ufo.read().expect("Lock Broken");
                let assoc = ufo_lock.config.associated_data();

                let core = ufo_lock.core.upgrade();
                if let Some(core) = core {
                    core.free(ufo).expect("error while freeing");
                }

                std::mem::drop(ufo);

                // only free this once the ufo is dropped
                // Not needed for correctness but to guard against anyone else trying to access it
                unsafe {
                    Box::from_raw(assoc.cast::<AssocData>());
                }
            }
        })
        .unwrap_or(())
    }

    #[no_mangle]
    pub extern "C" fn borough_is_error(&self) -> bool {
        self.deref().is_none()
    }

    #[no_mangle]
    pub extern "C" fn borough_reset(&self) {
        std::panic::catch_unwind(|| {
            let ufo = self.deref()?;
            let core = ufo.read().ok()?.core.upgrade()?;

            core.reset_impl(ufo).expect("Core error during reset");

            Some(())
        })
        .unwrap_or(None)
        .unwrap_or(())
    }

    #[no_mangle]
    pub extern "C" fn borough_params(&self, params_ptr: *mut BoroughParameters) {
        std::panic::catch_unwind(|| {
            let ufo = self.deref()?.read().ok()?;
            let assoc_data: &AssocData = unsafe { &*ufo.config.associated_data().cast() };

            let config = &ufo.config;
            let params: &mut BoroughParameters = unsafe { &mut *params_ptr.cast() };

            params.header_size = config.header_size();
            params.element_size = config.stride();
            params.element_ct = config.element_ct();
            params.min_load_ct = config.elements_loaded_at_once();
            params.populate_fn = assoc_data.0;
            params.populate_data = assoc_data.1;

            Some(())
        })
        .unwrap_or(None)
        .unwrap_or(())
    }

    #[no_mangle]
    pub extern "C" fn borough_read(&self, idx: usize, writeout: *mut libc::c_void) -> i32 {
        std::panic::catch_unwind(|| {
            let ufo = self.deref()?;
            let ufo_lock = ufo.read().expect("lock broken");
            let core = ufo_lock.core.upgrade()?;

            let stride = ufo_lock.config.stride();
            let ptr: *const libc::c_void = unsafe { ufo_lock.body_ptr().add(idx * stride) };
            let offset = UfoOffset::from_addr(&*ufo_lock, ptr);

            let chunk = core.populate_impl(ufo, &*ufo_lock, offset)
                .expect("error during populate");
            let _chunk = chunk
                .read()
                .expect("chunk lock broken");

            unsafe { memcpy(writeout, ptr, stride) };

            Some(0)
        })
        .unwrap_or(None)
        .unwrap_or(-1)
    }

    #[no_mangle]
    pub extern "C" fn borough_write(
        &self,
        idx: usize,
        data_to_write_back: *const libc::c_void,
    ) -> i32 {
        std::panic::catch_unwind(|| {
            let ufo = self.deref()?;
            let ufo_lock = ufo.read().expect("lock broken");
            let core = ufo_lock.core.upgrade()?;

            let stride = ufo_lock.config.stride();
            let ptr = unsafe { ufo_lock.body_ptr().add(idx * stride) };
            let offset = UfoOffset::from_addr(&*ufo_lock, ptr);

            let chunk = core.populate_impl(ufo, &*ufo_lock, offset)
            .expect("error during populate");
            let mut chunk = chunk
                .write()
                .expect("chunk lock broken");

            unsafe { memcpy(ptr, data_to_write_back, stride) };

            chunk.dirty = true;

            Some(0)
        })
        .unwrap_or(None)
        .unwrap_or(-1)
    }
}
