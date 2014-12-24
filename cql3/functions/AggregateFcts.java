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
package org.apache.cassandra.cql3.functions;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.LongType;

/**
 * Factory methods for aggregate functions.
 */
public abstract class AggregateFcts
{
    /**
     * The function used to count the number of rows of a result set. This function is called when COUNT(*) or COUNT(1)
     * is specified.
     */
    public static final AggregateFunction countRowsFunction =
            new NativeAggregateFunction("countRows", LongType.instance)
            {
                public Aggregate newAggregate()
                {
                    return new Aggregate()
                    {
                        private long count;

                        public void reset()
                        {
                            count = 0;
                        }

                        public ByteBuffer compute(int protocolVersion)
                        {
                            return ((LongType) returnType()).decompose(Long.valueOf(count));
                        }

                        public void addInput(int protocolVersion, List<ByteBuffer> values)
                        {
                            count++;
                        }
                    };
                }
            };

    /**
     * The SUM function for decimal values.
     */
    public static final AggregateFunction sumFunctionForDecimal =
            new NativeAggregateFunction("sum", DecimalType.instance, DecimalType.instance)
            {
                @Override
                public Aggregate newAggregate()
                {
                    return new Aggregate()
                    {
                        private BigDecimal sum = BigDecimal.ZERO;

                        public void reset()
                        {
                            sum = BigDecimal.ZERO;
                        }

                        public ByteBuffer compute(int protocolVersion)
                        {
                            return ((DecimalType) returnType()).decompose(sum);
                        }

                        public void addInput(int protocolVersion, List<ByteBuffer> values)
                        {
                            ByteBuffer value = values.get(0);

                            if (value == null)
                                return;

                            BigDecimal number = ((BigDecimal) argTypes().get(0).compose(value));
                            sum = sum.add(number);
                        }
                    };
                }
            };

    /**
     * The AVG function for decimal values.
     */
    public static final AggregateFunction avgFunctionForDecimal =
            new NativeAggregateFunction("avg", DecimalType.instance, DecimalType.instance)
            {
                public Aggregate newAggregate()
                {
                    return new Aggregate()
                    {
                        private BigDecimal sum = BigDecimal.ZERO;

                        private int count;

                        public void reset()
                        {
                            count = 0;
                            sum = BigDecimal.ZERO;
                        }

                        public ByteBuffer compute(int protocolVersion)
                        {
                            if (count == 0)
                                return ((DecimalType) returnType()).decompose(BigDecimal.ZERO);

                            return ((DecimalType) returnType()).decompose(sum.divide(BigDecimal.valueOf(count)));
                        }

                        public void addInput(int protocolVersion, List<ByteBuffer> values)
                        {
                            ByteBuffer value = values.get(0);

                            if (value == null)
                                return;

                            count++;
                            BigDecimal number = ((BigDecimal) argTypes().get(0).compose(value));
                            sum = sum.add(number);
                        }
                    };
                }
            };

    /**
     * The SUM function for varint values.
     */
    public static final AggregateFunction sumFunctionForVarint =
            new NativeAggregateFunction("sum", IntegerType.instance, IntegerType.instance)
            {
                public Aggregate newAggregate()
                {
                    return new Aggregate()
                    {
                        private BigInteger sum = BigInteger.ZERO;

                        public void reset()
                        {
                            sum = BigInteger.ZERO;
                        }

                        public ByteBuffer compute(int protocolVersion)
                        {
                            return ((IntegerType) returnType()).decompose(sum);
                        }

                        public void addInput(int protocolVersion, List<ByteBuffer> values)
                        {
                            ByteBuffer value = values.get(0);

                            if (value == null)
                                return;

                            BigInteger number = ((BigInteger) argTypes().get(0).compose(value));
                            sum = sum.add(number);
                        }
                    };
                }
            };

    /**
     * The AVG function for varint values.
     */
    public static final AggregateFunction avgFunctionForVarint =
            new NativeAggregateFunction("avg", IntegerType.instance, IntegerType.instance)
            {
                public Aggregate newAggregate()
                {
                    return new Aggregate()
                    {
                        private BigInteger sum = BigInteger.ZERO;

                        private int count;

                        public void reset()
                        {
                            count = 0;
                            sum = BigInteger.ZERO;
                        }

                        public ByteBuffer compute(int protocolVersion)
                        {
                            if (count == 0)
                                return ((IntegerType) returnType()).decompose(BigInteger.ZERO);

                            return ((IntegerType) returnType()).decompose(sum.divide(BigInteger.valueOf(count)));
                        }

                        public void addInput(int protocolVersion, List<ByteBuffer> values)
                        {
                            ByteBuffer value = values.get(0);

                            if (value == null)
                                return;

                            count++;
                            BigInteger number = ((BigInteger) argTypes().get(0).compose(value));
                            sum = sum.add(number);
                        }
                    };
                }
            };

    /**
     * The SUM function for int32 values.
     */
    public static final AggregateFunction sumFunctionForInt32 =
            new NativeAggregateFunction("sum", Int32Type.instance, Int32Type.instance)
            {
                public Aggregate newAggregate()
                {
                    return new Aggregate()
                    {
                        private int sum;

                        public void reset()
                        {
                            sum = 0;
                        }

                        public ByteBuffer compute(int protocolVersion)
                        {
                            return ((Int32Type) returnType()).decompose(sum);
                        }

                        public void addInput(int protocolVersion, List<ByteBuffer> values)
                        {
                            ByteBuffer value = values.get(0);

                            if (value == null)
                                return;

                            Number number = ((Number) argTypes().get(0).compose(value));
                            sum += number.intValue();
                        }
                    };
                }
            };

    /**
     * AVG function for int32 values.
     */
    public static final AggregateFunction avgFunctionForInt32 =
            new NativeAggregateFunction("avg", Int32Type.instance, Int32Type.instance)
            {
                public Aggregate newAggregate()
                {
                    return new Aggregate()
                    {
                        private int sum;

                        private int count;

                        public void reset()
                        {
                            count = 0;
                            sum = 0;
                        }

                        public ByteBuffer compute(int protocolVersion)
                        {
                            int avg = count == 0 ? 0 : sum / count;

                            return ((Int32Type) returnType()).decompose(avg);
                        }

                        public void addInput(int protocolVersion, List<ByteBuffer> values)
                        {
                            ByteBuffer value = values.get(0);

                            if (value == null)
                                return;

                            count++;
                            Number number = ((Number) argTypes().get(0).compose(value));
                            sum += number.intValue();
                        }
                    };
                }
            };

    /**
     * The SUM function for long values.
     */
    public static final AggregateFunction sumFunctionForLong =
            new NativeAggregateFunction("sum", LongType.instance, LongType.instance)
            {
                public Aggregate newAggregate()
                {
                    return new Aggregate()
                    {
                        private long sum;

                        public void reset()
                        {
                            sum = 0;
                        }

                        public ByteBuffer compute(int protocolVersion)
                        {
                            return ((LongType) returnType()).decompose(sum);
                        }

                        public void addInput(int protocolVersion, List<ByteBuffer> values)
                        {
                            ByteBuffer value = values.get(0);

                            if (value == null)
                                return;

                            Number number = ((Number) argTypes().get(0).compose(value));
                            sum += number.longValue();
                        }
                    };
                }
            };

    /**
     * AVG function for long values.
     */
    public static final AggregateFunction avgFunctionForLong =
            new NativeAggregateFunction("avg", LongType.instance, LongType.instance)
            {
                public Aggregate newAggregate()
                {
                    return new Aggregate()
                    {
                        private long sum;

                        private int count;

                        public void reset()
                        {
                            count = 0;
                            sum = 0;
                        }

                        public ByteBuffer compute(int protocolVersion)
                        {
                            long avg = count == 0 ? 0 : sum / count;

                            return ((LongType) returnType()).decompose(avg);
                        }

                        public void addInput(int protocolVersion, List<ByteBuffer> values)
                        {
                            ByteBuffer value = values.get(0);

                            if (value == null)
                                return;

                            count++;
                            Number number = ((Number) argTypes().get(0).compose(value));
                            sum += number.longValue();
                        }
                    };
                }
            };

    /**
     * The SUM function for float values.
     */
    public static final AggregateFunction sumFunctionForFloat =
            new NativeAggregateFunction("sum", FloatType.instance, FloatType.instance)
            {
                public Aggregate newAggregate()
                {
                    return new Aggregate()
                    {
                        private float sum;

                        public void reset()
                        {
                            sum = 0;
                        }

                        public ByteBuffer compute(int protocolVersion)
                        {
                            return ((FloatType) returnType()).decompose(sum);
                        }

                        public void addInput(int protocolVersion, List<ByteBuffer> values)
                        {
                            ByteBuffer value = values.get(0);

                            if (value == null)
                                return;

                            Number number = ((Number) argTypes().get(0).compose(value));
                            sum += number.floatValue();
                        }
                    };
                }
            };

    /**
     * AVG function for float values.
     */
    public static final AggregateFunction avgFunctionForFloat =
            new NativeAggregateFunction("avg", FloatType.instance, FloatType.instance)
            {
                public Aggregate newAggregate()
                {
                    return new Aggregate()
                    {
                        private float sum;

                        private int count;

                        public void reset()
                        {
                            count = 0;
                            sum = 0;
                        }

                        public ByteBuffer compute(int protocolVersion)
                        {
                            float avg = count == 0 ? 0 : sum / count;

                            return ((FloatType) returnType()).decompose(avg);
                        }

                        public void addInput(int protocolVersion, List<ByteBuffer> values)
                        {
                            ByteBuffer value = values.get(0);

                            if (value == null)
                                return;

                            count++;
                            Number number = ((Number) argTypes().get(0).compose(value));
                            sum += number.floatValue();
                        }
                    };
                }
            };

    /**
     * The SUM function for double values.
     */
    public static final AggregateFunction sumFunctionForDouble =
            new NativeAggregateFunction("sum", DoubleType.instance, DoubleType.instance)
            {
                public Aggregate newAggregate()
                {
                    return new Aggregate()
                    {
                        private double sum;

                        public void reset()
                        {
                            sum = 0;
                        }

                        public ByteBuffer compute(int protocolVersion)
                        {
                            return ((DoubleType) returnType()).decompose(sum);
                        }

                        public void addInput(int protocolVersion, List<ByteBuffer> values)
                        {
                            ByteBuffer value = values.get(0);

                            if (value == null)
                                return;

                            Number number = ((Number) argTypes().get(0).compose(value));
                            sum += number.doubleValue();
                        }
                    };
                }
            };

    /**
     * AVG function for double values.
     */
    public static final AggregateFunction avgFunctionForDouble =
            new NativeAggregateFunction("avg", DoubleType.instance, DoubleType.instance)
            {
                public Aggregate newAggregate()
                {
                    return new Aggregate()
                    {
                        private double sum;

                        private int count;

                        public void reset()
                        {
                            count = 0;
                            sum = 0;
                        }

                        public ByteBuffer compute(int protocolVersion)
                        {
                            double avg = count == 0 ? 0 : sum / count;

                            return ((DoubleType) returnType()).decompose(avg);
                        }

                        public void addInput(int protocolVersion, List<ByteBuffer> values)
                        {
                            ByteBuffer value = values.get(0);

                            if (value == null)
                                return;

                            count++;
                            Number number = ((Number) argTypes().get(0).compose(value));
                            sum += number.doubleValue();
                        }
                    };
                }
            };

    /**
     * Creates a MAX function for the specified type.
     *
     * @param inputType the function input and output type
     * @return a MAX function for the specified type.
     */
    public static AggregateFunction makeMaxFunction(final AbstractType<?> inputType)
    {
        return new NativeAggregateFunction("max", inputType, inputType)
        {
            public Aggregate newAggregate()
            {
                return new Aggregate()
                {
                    private ByteBuffer max;

                    public void reset()
                    {
                        max = null;
                    }

                    public ByteBuffer compute(int protocolVersion)
                    {
                        return max;
                    }

                    public void addInput(int protocolVersion, List<ByteBuffer> values)
                    {
                        ByteBuffer value = values.get(0);

                        if (value == null)
                            return;

                        if (max == null || returnType().compare(max, value) < 0)
                            max = value;
                    }
                };
            }
        };
    }

    /**
     * Creates a MIN function for the specified type.
     *
     * @param inputType the function input and output type
     * @return a MIN function for the specified type.
     */
    public static AggregateFunction makeMinFunction(final AbstractType<?> inputType)
    {
        return new NativeAggregateFunction("min", inputType, inputType)
        {
            public Aggregate newAggregate()
            {
                return new Aggregate()
                {
                    private ByteBuffer min;

                    public void reset()
                    {
                        min = null;
                    }

                    public ByteBuffer compute(int protocolVersion)
                    {
                        return min;
                    }

                    public void addInput(int protocolVersion, List<ByteBuffer> values)
                    {
                        ByteBuffer value = values.get(0);

                        if (value == null)
                            return;

                        if (min == null || returnType().compare(min, value) > 0)
                            min = value;
                    }
                };
            }
        };
    }

    /**
     * Creates a COUNT function for the specified type.
     *
     * @param inputType the function input type
     * @return a COUNT function for the specified type.
     */
    public static AggregateFunction makeCountFunction(AbstractType<?> inputType)
    {
        return new NativeAggregateFunction("count", LongType.instance, inputType)
        {
            public Aggregate newAggregate()
            {
                return new Aggregate()
                {
                    private long count;

                    public void reset()
                    {
                        count = 0;
                    }

                    public ByteBuffer compute(int protocolVersion)
                    {
                        return ((LongType) returnType()).decompose(count);
                    }

                    public void addInput(int protocolVersion, List<ByteBuffer> values)
                    {
                        ByteBuffer value = values.get(0);

                        if (value == null)
                            return;

                        count++;
                    }
                };
            }
        };
    }
}
