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

use std::fmt::Display;

use rand::Rng;
use shared_memory::{Shmem, ShmemConf, ShmemError};
use zenoh_result::{bail, zerror, ZResult};

const SEGMENT_DEDICATE_TRIES: usize = 100;

/// Segment of shared memory identified by an ID
pub struct Segment<ID> {
    pub shmem: Shmem,
    pub id: ID,
}

impl<ID> Segment<ID>
where
    rand::distributions::Standard: rand::distributions::Distribution<ID>,
    ID: Clone + Display,
{
    // Automatically generate free id and create a new segment identified by this id
    pub fn create(alloc_size: usize, id_prefix: &str) -> ZResult<Self> {
        for _ in 0..SEGMENT_DEDICATE_TRIES {
            // Generate random id
            let id: ID = rand::thread_rng().gen();

            // Try to create a new segment identified by prefix and generated id.
            // If creation fails because segment already exists for this id,
            // the creation attempt will be repeated with another id
            match ShmemConf::new()
                .size(alloc_size)
                .os_id(Self::os_id(id.clone(), id_prefix))
                .create()
            {
                Ok(shmem) => return Ok(Segment { shmem, id }),
                Err(ShmemError::LinkExists) => {}
                Err(ShmemError::MappingIdExists) => {}
                Err(e) => bail!("Unable to create POSIX shm segment: {}", e),
            }
        }
        bail!("Unable to dedicate POSIX shm segment file after {SEGMENT_DEDICATE_TRIES} tries!");
    }

    // Open an existing segment identified by id
    pub fn open(id: ID, id_prefix: &str) -> ZResult<Self> {
        let shmem = ShmemConf::new()
            .os_id(Self::os_id(id.clone(), id_prefix))
            .open()
            .map_err(|e| zerror!("Unable to open POSIX shm segment: {}", e))?;
        Ok(Self { shmem, id })
    }

    fn os_id(id: ID, id_prefix: &str) -> String {
        format!("{id_prefix}_{id}")
    }
}
