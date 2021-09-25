pub type BoroughPopulateData = *mut libc::c_void;
pub type BoroughPopulateCallout =
    extern "C" fn(BoroughPopulateData, usize, usize, *mut libc::c_uchar) -> i32;

#[repr(C)]
pub struct BoroughParameters {
    pub header_size: usize,
    pub element_size: usize,
    pub element_ct: usize,
    pub min_load_ct: usize,
    pub read_only: bool,
    pub populate_data: BoroughPopulateData,
    pub populate_fn: BoroughPopulateCallout,
}
