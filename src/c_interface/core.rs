use libc::c_void;
use std::sync::Arc;

use crate::errors::*;
use crate::ptr_hiding;
use crate::ufo_core;
use crate::ufo_core::*;
use crate::ufo_objects::*;

use super::object::*;
use super::prototype::*;

#[repr(C)]
pub struct NycCore {
    ptr: *mut c_void,
}
opaque_c_type!(NycCore, Arc<UfoCore>);

impl NycCore {
    #[no_mangle]
    pub unsafe extern "C" fn nyc_new_core(
        writeback_temp_path: *const libc::c_char,
        low_water_mark: usize,
        high_water_mark: usize,
    ) -> Self {
        std::panic::catch_unwind(|| {
            let wb = std::ffi::CStr::from_ptr(writeback_temp_path)
                .to_str()
                .expect("invalid string")
                .to_string();

            let mut low_water_mark = low_water_mark;
            let mut high_water_mark = high_water_mark;

            if low_water_mark > high_water_mark {
                std::mem::swap(&mut low_water_mark, &mut high_water_mark);
            }
            assert!(low_water_mark < high_water_mark);

            let config = UfoCoreConfig {
                writeback_temp_path: wb,
                low_watermark: low_water_mark,
                high_watermark: high_water_mark,
            };

            let core = ufo_core::UfoCore::new(config);
            match core {
                Err(_) => Self::none(),
                Ok(core) => Self::wrap(core),
            }
        })
        .unwrap_or_else(|_| Self::none())
    }

    #[no_mangle]
    pub extern "C" fn nyc_shutdown(self) {}

    #[no_mangle]
    pub extern "C" fn nyc_is_error(&self) -> bool {
        self.deref().is_none()
    }

    #[no_mangle]
    pub extern "C" fn nyc_new_borough(&self, prototype: &BoroughParameters) -> Borough {
        std::panic::catch_unwind(|| {
            let populate_data = prototype.populate_data as usize;
            let populate_fn = prototype.populate_fn;
            let populate = move |start, end, to_populate| {
                let ret = populate_fn(populate_data as *mut c_void, start, end, to_populate);

                if ret != 0 {
                    Err(UfoPopulateError)
                } else {
                    Ok(())
                }
            };

            let assoc_dat = ptr_hiding::as_c_void((prototype.populate_fn, prototype.populate_data));

            let params = UfoObjectParams {
                header_size: prototype.header_size,
                stride: prototype.element_size,
                element_ct: prototype.element_ct,
                min_load_ct: Some(prototype.min_load_ct).filter(|x| *x > 0),
                populate: Box::new(populate),
                associated_data: Box::into_raw(assoc_dat).cast(),
            };

            self.deref()
                .and_then(move |core| {
                    let ufo = ufo_core::UfoCore::allocate_ufo(core, params.new_config());
                    match ufo {
                        Ok(ufo) => Some(Borough::wrap(ufo)),
                        _ => None,
                    }
                })
                .unwrap_or_else(|| Borough::none())
        })
        .unwrap_or_else(|_| Borough::none())
    }
}
