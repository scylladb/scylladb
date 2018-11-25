# Scylla CQL extensions

Scylla extends the CQL language to provide a few extra features. This document
lists those extensions.

## WITH CACHE BYPASS

The `WITH CACHE BYPASS` clause informs the database that the data
being read is unlikely to be read again in the near future, and also
was unlikely to have been read in the near past; therefore no attempt
should be made to read it from the cache or to populate the cache with
the data. This is mostly useful for range scans; these typically
process large amounts of data with no temporal locality and do not
benefit from the cache.

The clause is placed immediately after the optional `ALLOW FILTERING`
clause:

    SELECT ... FROM ...
    WHERE ...
    ALLOW FILTERING          -- optional
    WITH CACHE BYPASS
