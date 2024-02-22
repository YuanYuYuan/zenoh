//
// Copyright (c) 2023 ZettaScale Technology
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License 2.0 which is available at
// http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
// which is available at https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
//
// Contributors:
//   ZettaScale Zenoh Team, <zenoh@zettascale.tech>
//

use std::{fmt::Display, ops::Deref};

use zenoh_result::{bail, ZResult};

use crate::SharedMemoryBuf;

use super::{chunk::AllocatedChunk, shared_memory_provider::LimitedAlignment};

// Allocation errors
#[derive(Debug)]
pub enum ZAllocError {
    NeedDefragment,             // defragmentation needed
    OutOfMemory,                // the provider is out of memory
    Other(zenoh_result::Error), // other error
}

impl From<zenoh_result::Error> for ZAllocError {
    fn from(value: zenoh_result::Error) -> Self {
        Self::Other(value)
    }
}

// alignemnt in powers of 2: 0 == 1-byte alignment, 1 == 2byte, 2 == 4byte, 4 == 8byte etc
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct AllocAlignment {
    pow: u32,
}

impl Display for AllocAlignment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("[{}]", self.get_alignment_value()))
    }
}

impl Default for AllocAlignment {
    fn default() -> Self {
        Self {
            pow: (std::mem::align_of::<u32>() as f64).sqrt() as u32 - 1,
        }
    }
}

impl AllocAlignment {
    pub fn new(pow: u32) -> Self {
        Self { pow }
    }

    pub fn get_alignment_value(&self) -> usize {
        1usize << self.pow
    }

    pub fn align_size(&self, size: usize) -> usize {
        let alignment = self.get_alignment_value();
        match size % alignment {
            0 => size,
            remainder => size + (alignment - remainder),
        }
    }
}

#[derive(Debug)]
pub struct MemoryLayout {
    size: usize,
    alignment: AllocAlignment,
}

impl Display for MemoryLayout {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "[size={},alignment={}]",
            self.size, self.alignment
        ))
    }
}

impl MemoryLayout {
    pub fn new(size: usize, alignment: AllocAlignment) -> ZResult<Self> {
        // size of an allocation must be a miltiple of it's alignment!
        match size % alignment.get_alignment_value() {
            0 => Ok(Self { size, alignment }),
            _ => bail!("size of an allocation must be a miltiple of it's alignment!"),
        }
    }

    pub fn size(&self) -> usize {
        self.size
    }
    pub fn alignment(&self) -> AllocAlignment {
        self.alignment
    }
}

#[derive(Debug)]
pub struct AllocLayout {
    pub layout: MemoryLayout,
}

impl Deref for AllocLayout {
    type Target = MemoryLayout;

    fn deref(&self) -> &Self::Target {
        &self.layout
    }
}

impl AllocLayout {
    pub fn new(
        size: usize,
        alignment: AllocAlignment,
        aligning: &impl LimitedAlignment,
    ) -> ZResult<Self> {
        // Create layout for the size corresponding to aligning entitie's capabilities
        if aligning.max_align() >= alignment {
            let layout = MemoryLayout::new(size, alignment)?;
            return Ok(Self { layout });
        }
        bail!("Unsupported alignemnt: {:?}", alignment)
    }
}

pub type ChunkAllocResult = Result<AllocatedChunk, ZAllocError>;
pub type BufAllocResult = Result<SharedMemoryBuf, ZAllocError>;
