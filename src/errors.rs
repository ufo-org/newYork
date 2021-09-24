use thiserror::Error;

use crate::bitmap::AddressOutOfRange;

#[derive(Error, Debug)]
pub enum UfoErr {
    #[error("Core shutdown")]
    CoreShutdown,
    #[error("Core non functional, {0}")]
    CoreBroken(String),
    #[error("Ufo Lock is broken")]
    UfoLockBroken,
    #[error("Ufo not found")]
    UfoNotFound,
    #[error("Ufo address error, internal math wrong?")]
    UfoAddressError,
    #[error("IO Error")]
    UfoIoError,
}

impl From<UfoAllocateErr> for UfoErr {
    fn from(uae: UfoAllocateErr) -> Self {
        match uae {
            UfoAllocateErr::UfoCoreLockBroken => UfoErr::UfoLockBroken,
            UfoAllocateErr::UfoCoreIoError => UfoErr::UfoIoError,
        }
    }
}

impl From<std::io::Error> for UfoErr {
    fn from(_: std::io::Error) -> Self {
        UfoErr::UfoIoError
    }
}

impl<T> From<std::sync::PoisonError<T>> for UfoErr {
    fn from(_: std::sync::PoisonError<T>) -> Self {
        UfoErr::UfoLockBroken
    }
}

impl<T> From<std::sync::mpsc::SendError<T>> for UfoErr {
    fn from(_e: std::sync::mpsc::SendError<T>) -> Self {
        UfoErr::CoreBroken("Error when sending messsge to the core".into())
    }
}

impl<T> From<crossbeam::channel::SendError<T>> for UfoErr {
    fn from(_e: crossbeam::channel::SendError<T>) -> Self {
        UfoErr::CoreBroken("Error when sending messsge to the core".into())
    }
}

impl From<AddressOutOfRange> for UfoErr {
    fn from(_: AddressOutOfRange) -> Self {
        UfoErr::UfoAddressError
    }
}

#[derive(Error, Debug)]
pub enum UfoAllocateErr {
    #[error("Ufo Lock is broken")]
    UfoCoreLockBroken,
    #[error("IO Error")]
    UfoCoreIoError,
}

impl<T> From<std::sync::PoisonError<T>> for UfoAllocateErr {
    fn from(_: std::sync::PoisonError<T>) -> Self {
        UfoAllocateErr::UfoCoreLockBroken
    }
}

impl From<std::io::Error> for UfoAllocateErr {
    fn from(_: std::io::Error) -> Self {
        UfoAllocateErr::UfoCoreIoError
    }
}
