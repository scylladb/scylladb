namespace query {

class qr_cell stub [[writable]] {
    std::experimental::optional<api::timestamp_type> timestamp; // present when send_timestamp option set in partition_slice
    std::experimental::optional<gc_clock::time_point> expiry; // present when send_expiry option set in partition_slice

    // Specified by CQL binary protocol, according to cql_serialization_format in read_command.
    bytes value;
};

class qr_row stub [[writable]] {
    std::vector<std::experimental::optional<qr_cell>> cells; // ordered as requested in partition_slice
};

class qr_clustered_row stub [[writable]] {
    std::experimental::optional<clustering_key> key; // present when send_clustering_key option set in partition_slice
    qr_row cells; // ordered as requested in partition_slice
};

class qr_partition stub [[writable]] {
    std::experimental::optional<partition_key> key; // present when send_partition_key option set in partition_slice
    qr_row static_row;
    std::vector<qr_clustered_row> rows; // ordered by key
};

class query_result stub [[writable]] {
    std::vector<qr_partition> partitions; // in ring order
};

}
