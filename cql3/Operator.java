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
package org.apache.cassandra.cql3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public enum Operator
{
    EQ(0)
    {
        @Override
        public String toString()
        {
            return "=";
        }
    },
    LT(4)
    {
        @Override
        public String toString()
        {
            return "<";
        }

        @Override
        public Operator reverse()
        {
            return GT;
        }
    },
    LTE(3)
    {
        @Override
        public String toString()
        {
            return "<=";
        }

        @Override
        public Operator reverse()
        {
            return GTE;
        }
    },
    GTE(1)
    {
        @Override
        public String toString()
        {
            return ">=";
        }

        @Override
        public Operator reverse()
        {
            return LTE;
        }
    },
    GT(2)
    {
        @Override
        public String toString()
        {
            return ">";
        }

        @Override
        public Operator reverse()
        {
            return LT;
        }
    },
    IN(7)
    {
    },
    CONTAINS(5)
    {
    },
    CONTAINS_KEY(6)
    {
        @Override
        public String toString()
        {
            return "CONTAINS KEY";
        }
    },
    NEQ(8)
    {
        @Override
        public String toString()
        {
            return "!=";
        }
    };

    /**
     * The binary representation of this <code>Enum</code> value.
     */
    private final int b;

    /**
     * Creates a new <code>Operator</code> with the specified binary representation.
     * @param b the binary representation of this <code>Enum</code> value
     */
    private Operator(int b)
    {
        this.b = b;
    }

    /**
     * Write the serialized version of this <code>Operator</code> to the specified output.
     *
     * @param output the output to write to
     * @throws IOException if an I/O problem occurs while writing to the specified output
     */
    public void writeTo(DataOutput output) throws IOException
    {
        output.writeInt(b);
    }

    /**
     * Deserializes a <code>Operator</code> instance from the specified input.
     *
     * @param input the input to read from
     * @return the <code>Operator</code> instance deserialized
     * @throws IOException if a problem occurs while deserializing the <code>Type</code> instance.
     */
    public static Operator readFrom(DataInput input) throws IOException
    {
          int b = input.readInt();
          for (Operator operator : values())
              if (operator.b == b)
                  return operator;

          throw new IOException(String.format("Cannot resolve Relation.Type from binary representation: %s", b));
    }

    @Override
    public String toString()
    {
         return this.name();
    }

    /**
     * Returns the reverse operator if this one.
     *
     * @return the reverse operator of this one.
     */
    public Operator reverse()
    {
        return this;
    }
}
