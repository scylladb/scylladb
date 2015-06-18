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

import java.io.Serializable;
import java.net.InetAddress;

import com.google.common.base.Objects;

/**
 * ProgressInfo contains file transfer progress.
 */
public class ProgressInfo implements Serializable
{
    /**
     * Direction of the stream.
     */
    public static enum Direction
    {
        OUT(0),
        IN(1);

        public final byte code;

        private Direction(int code)
        {
            this.code = (byte) code;
        }

        public static Direction fromByte(byte direction)
        {
            return direction == 0 ? OUT : IN;
        }
    }

    public final InetAddress peer;
    public final int sessionIndex;
    public final String fileName;
    public final Direction direction;
    public final long currentBytes;
    public final long totalBytes;

    public ProgressInfo(InetAddress peer, int sessionIndex, String fileName, Direction direction, long currentBytes, long totalBytes)
    {
        assert totalBytes > 0;

        this.peer = peer;
        this.sessionIndex = sessionIndex;
        this.fileName = fileName;
        this.direction = direction;
        this.currentBytes = currentBytes;
        this.totalBytes = totalBytes;
    }

    /**
     * @return true if file transfer is completed
     */
    public boolean isCompleted()
    {
        return currentBytes >= totalBytes;
    }

    /**
     * ProgressInfo is considered to be equal only when all attributes except currentBytes are equal.
     */
    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ProgressInfo that = (ProgressInfo) o;

        if (totalBytes != that.totalBytes) return false;
        if (direction != that.direction) return false;
        if (!fileName.equals(that.fileName)) return false;
        if (sessionIndex != that.sessionIndex) return false;
        return peer.equals(that.peer);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(peer, sessionIndex, fileName, direction, totalBytes);
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder(fileName);
        sb.append(" ").append(currentBytes);
        sb.append("/").append(totalBytes).append(" bytes");
        sb.append("(").append(currentBytes*100/totalBytes).append("%) ");
        sb.append(direction == Direction.OUT ? "sent to " : "received from ");
        sb.append("idx:").append(sessionIndex);
        sb.append(peer);
        return sb.toString();
    }
}
