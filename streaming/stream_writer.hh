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

#include "streaming/stream_session.hh"
#include "sstables/sstables.hh"
#include <map>

namespace streaming {
/**
 * StreamWriter writes given section of the SSTable to given channel.
 */
class stream_writer {
    static constexpr int DEFAULT_CHUNK_SIZE = 64 * 1024;
protected:
    sstables::sstable& sstable;
    std::map<int64_t, int64_t> sections;
    //StreamRateLimiter limiter;
    stream_session& session;
#if 0
    private OutputStream compressedOutput;
    // allocate buffer to use for transfers only once
    private byte[] transferBuffer;
#endif

public:
    stream_writer(sstables::sstable& sstable_, std::map<int64_t, int64_t> sections_, stream_session& session_)
        : sstable(sstable_)
        , sections(std::move(sections_))
        , session(session_) {
        //this.limiter =  StreamManager.getRateLimiter(session.peer);
    }
#if 0
    /**
     * Stream file of specified sections to given channel.
     *
     * StreamWriter uses LZF compression on wire to decrease size to transfer.
     *
     * @param channel where this writes data to
     * @throws IOException on any I/O error
     */
    public void write(WritableByteChannel channel) throws IOException
    {
        long totalSize = totalSize();
        RandomAccessReader file = sstable.openDataReader();
        ChecksumValidator validator = new File(sstable.descriptor.filenameFor(Component.CRC)).exists()
                                    ? DataIntegrityMetadata.checksumValidator(sstable.descriptor)
                                    : null;
        transferBuffer = validator == null ? new byte[DEFAULT_CHUNK_SIZE] : new byte[validator.chunkSize];

        // setting up data compression stream
        compressedOutput = new LZFOutputStream(Channels.newOutputStream(channel));
        long progress = 0L;

        try
        {
            // stream each of the required sections of the file
            for (Pair<Long, Long> section : sections)
            {
                long start = validator == null ? section.left : validator.chunkStart(section.left);
                int readOffset = (int) (section.left - start);
                // seek to the beginning of the section
                file.seek(start);
                if (validator != null)
                    validator.seek(start);

                // length of the section to read
                long length = section.right - start;
                // tracks write progress
                long bytesRead = 0;
                while (bytesRead < length)
                {
                    long lastBytesRead = write(file, validator, readOffset, length, bytesRead);
                    bytesRead += lastBytesRead;
                    progress += (lastBytesRead - readOffset);
                    session.progress(sstable.descriptor, ProgressInfo.Direction.OUT, progress, totalSize);
                    readOffset = 0;
                }

                // make sure that current section is send
                compressedOutput.flush();
            }
        }
        finally
        {
            // no matter what happens close file
            FileUtils.closeQuietly(file);
            FileUtils.closeQuietly(validator);
        }
    }
#endif

protected:
    int64_t total_size();

#if 0
    /**
     * Sequentially read bytes from the file and write them to the output stream
     *
     * @param reader The file reader to read from
     * @param validator validator to verify data integrity
     * @param start number of bytes to skip transfer, but include for validation.
     * @param length The full length that should be read from {@code reader}
     * @param bytesTransferred Number of bytes already read out of {@code length}
     *
     * @return Number of bytes read
     *
     * @throws java.io.IOException on any I/O error
     */
    protected long write(RandomAccessReader reader, ChecksumValidator validator, int start, long length, long bytesTransferred) throws IOException
    {
        int toTransfer = (int) Math.min(transferBuffer.length, length - bytesTransferred);
        int minReadable = (int) Math.min(transferBuffer.length, reader.length() - reader.getFilePointer());

        reader.readFully(transferBuffer, 0, minReadable);
        if (validator != null)
            validator.validate(transferBuffer, 0, minReadable);

        limiter.acquire(toTransfer - start);
        compressedOutput.write(transferBuffer, start, (toTransfer - start));

        return toTransfer;
    }
#endif
};

} // namespace streaming
