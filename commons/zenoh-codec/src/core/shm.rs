//
// Copyright (c) 2022 ZettaScale Technology
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
use crate::{RCodec, WCodec, Zenoh080};
use zenoh_buffers::{
    reader::{DidntRead, Reader},
    writer::{DidntWrite, Writer},
};
use zenoh_shm::{
    api::provider::chunk::ChunkDescriptor, header::descriptor::HeaderDescriptor,
    posix_shm::segment, watchdog::descriptor::Descriptor, SharedMemoryBufInfo,
};

impl<W> WCodec<&Descriptor, &mut W> for Zenoh080
where
    W: Writer,
{
    type Output = Result<(), DidntWrite>;

    fn write(self, writer: &mut W, x: &Descriptor) -> Self::Output {
        self.write(&mut *writer, x.id)?;
        self.write(&mut *writer, x.index_and_bitpos)?;
        Ok(())
    }
}

impl<W> WCodec<&HeaderDescriptor, &mut W> for Zenoh080
where
    W: Writer,
{
    type Output = Result<(), DidntWrite>;

    fn write(self, writer: &mut W, x: &HeaderDescriptor) -> Self::Output {
        self.write(&mut *writer, x.id)?;
        self.write(&mut *writer, x.index)?;
        Ok(())
    }
}

impl<W> WCodec<&ChunkDescriptor, &mut W> for Zenoh080
where
    W: Writer,
{
    type Output = Result<(), DidntWrite>;

    fn write(self, writer: &mut W, x: &ChunkDescriptor) -> Self::Output {
        self.write(&mut *writer, x.segment)?;
        self.write(&mut *writer, x.chunk)?;
        self.write(&mut *writer, x.len)?;
        Ok(())
    }
}

impl<W> WCodec<&SharedMemoryBufInfo, &mut W> for Zenoh080
where
    W: Writer,
{
    type Output = Result<(), DidntWrite>;

    fn write(self, writer: &mut W, x: &SharedMemoryBufInfo) -> Self::Output {
        let SharedMemoryBufInfo {
            watchdog_descriptor,
            header_descriptor,
            generation,
            data_descriptor,
            shm_protocol,
            data_len,
        } = x;

        self.write(&mut *writer, watchdog_descriptor)?;
        self.write(&mut *writer, header_descriptor)?;
        self.write(&mut *writer, generation)?;
        self.write(&mut *writer, data_descriptor)?;
        self.write(&mut *writer, shm_protocol)?;
        self.write(&mut *writer, data_len)?;
        Ok(())
    }
}

impl<R> RCodec<Descriptor, &mut R> for Zenoh080
where
    R: Reader,
{
    type Error = DidntRead;

    fn read(self, reader: &mut R) -> Result<Descriptor, Self::Error> {
        let id = self.read(&mut *reader)?;
        let index_and_bitpos = self.read(&mut *reader)?;

        Ok(Descriptor {
            id,
            index_and_bitpos,
        })
    }
}

impl<R> RCodec<HeaderDescriptor, &mut R> for Zenoh080
where
    R: Reader,
{
    type Error = DidntRead;

    fn read(self, reader: &mut R) -> Result<HeaderDescriptor, Self::Error> {
        let id = self.read(&mut *reader)?;
        let index = self.read(&mut *reader)?;

        Ok(HeaderDescriptor { id, index })
    }
}

impl<R> RCodec<ChunkDescriptor, &mut R> for Zenoh080
where
    R: Reader,
{
    type Error = DidntRead;

    fn read(self, reader: &mut R) -> Result<ChunkDescriptor, Self::Error> {
        let segment = self.read(&mut *reader)?;
        let chunk = self.read(&mut *reader)?;
        let len = self.read(&mut *reader)?;

        Ok(ChunkDescriptor {
            segment,
            chunk,
            len,
        })
    }
}

impl<R> RCodec<SharedMemoryBufInfo, &mut R> for Zenoh080
where
    R: Reader,
{
    type Error = DidntRead;

    fn read(self, reader: &mut R) -> Result<SharedMemoryBufInfo, Self::Error> {
        let watchdog_descriptor = self.read(&mut *reader)?;
        let header_descriptor = self.read(&mut *reader)?;
        let generation = self.read(&mut *reader)?;
        let data_descriptor = self.read(&mut *reader)?;
        let shm_protocol = self.read(&mut *reader)?;
        let data_len = self.read(&mut *reader)?;

        let shm_info = SharedMemoryBufInfo::new(
            watchdog_descriptor,
            header_descriptor,
            generation,
            data_descriptor,
            shm_protocol,
            data_len,
        );
        Ok(shm_info)
    }
}
