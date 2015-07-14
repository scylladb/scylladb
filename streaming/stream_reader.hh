/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Modified by Cloudius Systems.
 * Copyright 2015 Cloudius Systems.
 */

#pragma once

#include "utils/UUID.hh"
#include "streaming/stream_session.hh"
#include "sstables/sstables.hh"
#include "streaming/messages/file_message_header.hh"
#include <map>

namespace streaming {
/**
 * StreamReader reads from stream and writes to SSTable.
 */
class stream_reader {
    using UUID = utils::UUID;
    using format_types = sstables::sstable::format_types;
    using version_types = sstables::sstable::version_types;
    using file_message_header = streaming::messages::file_message_header;
protected:
    UUID cf_id;
    int64_t estimated_keys;
    std::map<int64_t, int64_t> sections;
    shared_ptr<stream_session> session;
    // FIXME: Version
    version_types input_version;
    int64_t repaired_at;
    format_types format;
    int sstable_level;
    // FIXME: Descriptor
    //Descriptor desc;
public:
    stream_reader(file_message_header header, shared_ptr<stream_session> session_);
    ~stream_reader();
#if 0

    /**
     * @param channel where this reads data from
     * @return SSTable transferred
     * @throws IOException if reading the remote sstable fails. Will throw an RTE if local write fails.
     */
    public SSTableWriter read(ReadableByteChannel channel) throws IOException
    {
        logger.debug("reading file from {}, repairedAt = {}, level = {}", session.peer, repaired_at, sstableLevel);
        long totalSize = totalSize();

        Pair<String, String> kscf = Schema.instance.getCF(cf_id);
        if (kscf == null)
        {
            // schema was dropped during streaming
            throw new IOException("CF " + cf_id + " was dropped during streaming");
        }
        ColumnFamilyStore cfs = Keyspace.open(kscf.left).getColumnFamilyStore(kscf.right);

        SSTableWriter writer = createWriter(cfs, totalSize, repaired_at, format);

        DataInputStream dis = new DataInputStream(new LZFInputStream(Channels.newInputStream(channel)));
        BytesReadTracker in = new BytesReadTracker(dis);
        try
        {
            while (in.getBytesRead() < totalSize)
            {
                writeRow(writer, in, cfs);

                // TODO move this to BytesReadTracker
                session.progress(desc, ProgressInfo.Direction.IN, in.getBytesRead(), totalSize);
            }
            return writer;
        } catch (Throwable e)
        {
            writer.abort();
            drain(dis, in.getBytesRead());
            if (e instanceof IOException)
                throw (IOException) e;
            else
                throw Throwables.propagate(e);
        }
    }

    protected SSTableWriter createWriter(ColumnFamilyStore cfs, long totalSize, long repaired_at, SSTableFormat.Type format) throws IOException
    {
        Directories.DataDirectory localDir = cfs.directories.getWriteableLocation(totalSize);
        if (localDir == null)
            throw new IOException("Insufficient disk space to store " + totalSize + " bytes");
        desc = Descriptor.fromFilename(cfs.getTempSSTablePath(cfs.directories.getLocationForDisk(localDir), format));

        return SSTableWriter.create(desc, estimated_keys, repaired_at, sstableLevel);
    }

    protected void drain(InputStream dis, long bytesRead) throws IOException
    {
        long toSkip = totalSize() - bytesRead;

        // InputStream.skip can return -1 if dis is inaccessible.
        long skipped = dis.skip(toSkip);
        if (skipped == -1)
            return;

        toSkip = toSkip - skipped;
        while (toSkip > 0)
        {
            skipped = dis.skip(toSkip);
            if (skipped == -1)
                break;
            toSkip = toSkip - skipped;
        }
    }
#endif
protected:
    int64_t total_size();

#if 0
    protected void writeRow(SSTableWriter writer, DataInput in, ColumnFamilyStore cfs) throws IOException
    {
        DecoratedKey key = StorageService.getPartitioner().decorateKey(ByteBufferUtil.readWithShortLength(in));
        writer.appendFromStream(key, cfs.metadata, in, input_version);
        cfs.invalidateCachedRow(key);
    }
#endif
};

} // namespace streaming
