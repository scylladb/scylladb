# CQL3 Type Mapping

| CQL3 type | Scylla type class      | Scylla `data_type` instance | Cassandra type class                               |
|-----------|------------------------|-----------------------------|----------------------------------------------------|
| ascii     | ascii_type_impl        | ascii_type                  | org.apache.cassandra.db.marshal.AsciiType          |
| bigint    | long_type_impl         | long_type                   | org.apache.cassandra.db.marshal.LongType           |
| blob      | bytes_type_impl        | bytes_type                  | org.apache.cassandra.db.marshal.BytesType          |
| boolean   | boolean_type_impl      | boolean_type                | org.apache.cassandra.db.marshal.BooleanType        |
| counter   | counter_type_impl      | counter_type                | org.apache.cassandra.db.marshal.CounterColumnType  |
| date      | simple_date_type_impl  | simple_date_type            | org.apache.cassandra.db.marshal.SimpleDateType     |
| decimal   | decimal_type_impl      | decimal_type                | org.apache.cassandra.db.marshal.DecimalType        |
| double    | double_type_impl       | double_type                 | org.apache.cassandra.db.marshal.DoubleType         |
| duration  | duration_type_impl     | duration_type               | org.apache.cassandra.db.marshal.DurationType       |
| float     | float_type_impl        | float_type                  | org.apache.cassandra.db.marshal.FloatType          |
| inet      | inet_addr_type_impl    | inet_addr_type              | org.apache.cassandra.db.marshal.InetAddressType    |
| int       | int32_type_impl        | int32_type                  | org.apache.cassandra.db.marshal.Int32Type          |
| smallint  | short_type_impl        | short_type                  | org.apache.cassandra.db.marshal.ShortType          |
| text      | utf8_type_impl         | utf8_type                   | org.apache.cassandra.db.marshal.UTF8Type           |
| time      | time_type_impl         | time_type                   | org.apache.cassandra.db.marshal.TimeType           |
| timestamp | imestamp_type_impl     | timestamp_type              | org.apache.cassandra.db.marshal.TimestampType      |
| timeuuid  | timeuuid_type_impl     | timeuuid_type               | org.apache.cassandra.db.marshal.TimeUUIDType       |
| tinyint   | byte_type_impl         | byte_type                   | org.apache.cassandra.db.marshal.ByteType           |
| uuid      | uuid_type_impl         | uuid_type                   | org.apache.cassandra.db.marshal.UUIDType           |
| varint    | varint_type_impl       | varint_type                 | org.apache.cassandra.db.marshal.IntegerType        |
| list      | list_type_impl         | n/a                         | org.apache.cassandra.db.marshal.ListType           |
| map       | map_type_impl          | n/a                         | org.apache.cassandra.db.marshal.MapType            |
| set       | set_type_impl          | n/a                         | org.apache.cassandra.db.marshal.SetType            |
| tuple     | tuple_type_impl        | n/a                         | org.apache.cassandra.db.marshal.TupleType          |
| UDT       | user_type_impl         | n/a                         | org.apache.cassandra.db.marshal.UserType           |
| frozen    | n/a                    | n/a                         | org.apache.cassandra.db.marshal.FrozenType         |
| n/a       | empty_type_impl        | empty_type                  | org.apache.cassandra.db.marshal.EmptyType          |
| n/a       | reversed_type_impl     | n/a                         | org.apache.cassandra.db.marshal.ReversedType       |
