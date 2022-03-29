use crate::prelude::*;

use std::fmt::{Debug, Formatter};
use std::ops::{Deref, DerefMut};

/// Dirty wraps a value of type T with functions similiar to that of a Read/Write
/// lock but simply sets a dirty flag on write(), reset on read()
pub struct Dirty<T: Clone + Debug> {
    value: T,
    dirty: bool,
}

impl<T: Clone + Debug> Dirty<T> {
    /// Create a new Dirty
    pub fn new(val: T) -> Dirty<T> {
        Dirty {
            value: val,
            dirty: false,
        }
    }

    /// Create a new Dirty with dirty set
    pub fn new_dirty(val: T) -> Dirty<T> {
        Dirty {
            value: val,
            dirty: true,
        }
    }

    /// Returns true if dirty, false otherwise
    #[allow(dead_code)]
    pub fn dirty(&self) -> bool {
        self.dirty
    }

    /// Reset dirty
    pub fn sync(&mut self) {
        self.dirty = false;
    }
}

impl<T: Clone + Debug> Deref for Dirty<T> {
    type Target = T;

    /// Read the value
    fn deref(&self) -> &T {
        &self.value
    }
}

impl<T: Clone + Debug> DerefMut for Dirty<T> {
    /// Writable value return, sets the dirty flag
    fn deref_mut(&mut self) -> &mut T {
        self.dirty = true;
        &mut self.value
    }
}

impl<T: Clone + Debug> Drop for Dirty<T> {
    /// Guard it is not dirty when dropping
    fn drop(&mut self) {
        debug_assert!(!self.dirty, "data dirty when dropping");
    }
}

impl<T: Clone + Debug> Debug for Dirty<T> {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        let tag = if self.dirty { "Dirty" } else { "Clean" };
        write!(f, "[{}] {:?}", tag, self.value)
    }
}

/// Helper Iterator to iterate subranges for each block
pub struct BlockRangeIter {
    pub begin: usize,
    pub end: usize,
    pub block_size: usize,
}

impl Iterator for BlockRangeIter {
    type Item = BlockRange;

    fn next(&mut self) -> Option<<Self as Iterator>::Item> {
        // Exit condition
        if self.begin >= self.end {
            return None;
        }

        // fetch subrange
        let sub_range = {
            let block_id = self.begin / self.block_size;
            let begin = self.begin % self.block_size;
            let end = if block_id == self.end / self.block_size {
                self.end % self.block_size
            } else {
                self.block_size
            };
            BlockRange {
                block_id,
                begin,
                end,
            }
        };

        self.begin += sub_range.len();
        Some(sub_range)
    }
}

/// Describe the range for one block
pub struct BlockRange {
    pub block_id: BlockId,
    pub begin: usize,
    pub end: usize,
}

impl BlockRange {
    pub fn len(&self) -> usize {
        self.end - self.begin
    }
}
