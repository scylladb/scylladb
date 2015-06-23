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
 */
package org.apache.cassandra.streaming;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.Collection;

import com.ning.compress.lzf.LZFOutputStream;

import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.DataIntegrityMetadata;
import org.apache.cassandra.io.util.DataIntegrityMetadata.ChecksumValidator;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.streaming.StreamManager.StreamRateLimiter;
import org.apache.cassandra.utils.Pair;

/**
 * StreamWriter writes given section of the SSTable to given channel.
 */
public class StreamWriter
{
    private static final int DEFAULT_CHUNK_SIZE = 64 * 1024;

    protected final SSTableReader sstable;
    protected final Collection<Pair<Long, Long>> sections;
    protected final StreamRateLimiter limiter;
    protected final StreamSession session;

    private OutputStream compressedOutput;

    // allocate buffer to use for transfers only once
    private byte[] transferBuffer;

    public StreamWriter(SSTableReader sstable, Collection<Pair<Long, Long>> sections, StreamSession session)
    {
        this.session = session;
        this.sstable = sstable;
        this.sections = sections;
        this.limiter =  StreamManager.getRateLimiter(session.peer);
    }

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

    protected long totalSize()
    {
        long size = 0;
        for (Pair<Long, Long> section : sections)
            size += section.right - section.left;
        return size;
    }

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
}
