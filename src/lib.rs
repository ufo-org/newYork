#![feature(
    ptr_internals,
    once_cell,
    slice_ptr_get,
    mutex_unlock,
    thread_id_value,
    int_roundings,
    negative_impls,
)]

mod bitmap;
mod bitwise_spinlock;
mod errors;
mod math;
mod mmap_wrapers;
mod once_await;
mod return_checks;
mod ufo_core;
mod ufo_objects;
mod write_buffer;

pub use ufo_objects::*;

mod c_interface;

pub use c_interface::core::*;
pub use c_interface::object::*;