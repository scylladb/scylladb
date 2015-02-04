/*
 * Copyright 2015 Cloudius Systems
 */

#include "cql3/column_identifier.hh"

namespace cql3 {

::shared_ptr<column_identifier> prepare_column_identifier(schema_ptr s) {
#if 0
            AbstractType<?> comparator = cfm.comparator.asAbstractType();
            if (cfm.getIsDense() || comparator instanceof CompositeType || comparator instanceof UTF8Type)
                return new ColumnIdentifier(text, true);

            // We have a Thrift-created table with a non-text comparator.  We need to parse column names with the comparator
            // to get the correct ByteBuffer representation.  However, this doesn't apply to key aliases, so we need to
            // make a special check for those and treat them normally.  See CASSANDRA-8178.
            ByteBuffer bufferName = ByteBufferUtil.bytes(text);
            for (ColumnDefinition def : cfm.partitionKeyColumns())
            {
                if (def.name.bytes.equals(bufferName))
                    return new ColumnIdentifier(text, true);
            }
            return new ColumnIdentifier(comparator.fromString(rawText), text);
#endif
    throw std::runtime_error("not implemented");
}

std::ostream& operator<<(std::ostream& out, const column_identifier::raw& id) {
    return out << id._text;
}

}
