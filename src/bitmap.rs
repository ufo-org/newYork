use thiserror::Error;

pub struct Bitmap {
    size: usize,
    map: Vec<u8>,
}

#[derive(Error, Debug)]
#[error("Out of range, {0}")]
pub struct AddressOutOfRange(usize);

impl Bitmap {
    pub fn new(size_bits: usize) -> Self {
        let whole_bytes = size_bits >> 3;
        let spare_bits = size_bits & 0b111;
        let total_bytes = if spare_bits > 0 {
            whole_bytes + 1
        } else {
            whole_bytes
        };

        Bitmap {
            size: size_bits,
            map: Vec::with_capacity(total_bytes),
        }
    }

    pub fn test(&self, addr: usize) -> Result<bool, AddressOutOfRange> {
        if addr >= self.size {
            return Err(AddressOutOfRange(addr));
        }

        let byte = addr >> 3;
        let bit = addr & 0b111;

        Ok(1 == ((self.map[byte] >> bit) & 1))
    }

    pub fn set(&mut self, addr: usize, value: bool) -> Result<bool, AddressOutOfRange> {
        if addr >= self.size {
            return Err(AddressOutOfRange(addr));
        }

        let byte = addr >> 3;
        let bit = addr & 0b111;

        let b = self.map[byte];
        let old = 1 == ((b >> bit) & 1);

        let the_bit = 1 << bit;
        if value {
            self.map[byte] = b | the_bit;
        } else {
            self.map[byte] = b & !the_bit;
        }

        Ok(old)
    }
}
