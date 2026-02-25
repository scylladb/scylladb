--
-- Copyright (C) 2025-present ScyllaDB
--
-- SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
--

-- Produce a histogram of write times (timestamps) in the sstable(s).
--
-- Crawl over all timestamps in the data component and add them to a histogram.
-- The bucket size by default is a month, tunable with the --bucket option.
-- The timestamp of all objects that have one are added to the histogram:
-- * cells (atomic and collection cells)
-- * tombstones (partition-tombstone, range-tombstone, row-tombstone,
--   shadowable-tombstone, cell-tombstone, collection-tombstone, cell-tombstone)
-- * row-marker
-- 
-- This allows determining when the data was written, provided the writer of the
-- data didn't mangle with the timestamps.
-- This produces two lines of output:
-- 1) Stats about the histogram and the collected data.
-- 2) A JSON object containing the histogram, with two arrays: "buckets" and "counts".
--
-- following example python script:
-- 
--      import datetime
--      import json
--      import matplotlib.pyplot as plt # requires the matplotlib python package
-- 
--      with open('histogram.json', 'r') as f:
--          data = json.load(f)
-- 
--      x = data['buckets']
--      y = data['counts']
-- 
--      max_y = max(y)
-- 
--      x = [datetime.date.fromtimestamp(i / 1000000).strftime('%Y.%m') for i in x]
--      y = [i / max_y for i in y]
-- 
--      fig, ax = plt.subplots()
-- 
--      ax.set_xlabel('Timestamp')
--      ax.set_ylabel('Normalized cell count')
--      ax.set_title('Histogram of data write-time')
--      ax.bar(x, y)
-- 
--      plt.show()

-- the unit of time to use as bucket, one of (years, months, weeks, days, hours)
BUCKET = "months"

partitions = 0
rows = 0
cells = 0
timestamps = 0
histogram = {}

function timestamp_bucket(ts)
    date = os.date("*t", ts // 1000000)
    bucket_start_date = {}
    if BUCKET == "years" then
        bucket_start_date = {year = date.year, month = 1, day = 1}
    elseif BUCKET == "months" then
        bucket_start_date = {year = date.year, month = date.month, day = 1}
    elseif BUCKET == "weeks" then
        bucket_start_date = {year = date.year, month = date.month, day = 1 + date.day // 7}
    elseif BUCKET == "days" then
        bucket_start_date = {year = date.year, month = date.month, day = date.day}
    elseif BUCKET == "hours" then
        bucket_start_date = {year = date.year, month = date.month, day = date.day, hour = date.hour}
    else
        error("Invalid BUCKET value: " .. BUCKET)
    end

    return os.time(bucket_start_date) * 1000000
end

function collect_timestamp(ts)
    ts = timestamp_bucket(ts)

    timestamps = timestamps + 1

    if histogram[ts] == nil then
        histogram[ts] = 1
    else
        histogram[ts] = histogram[ts] + 1
    end
end

function collect_column(cell)
    if cell.type == "collection" then
        if cell.tombstone then
            collect_timestamp(cell.tombstone.timestamp)
        end
        for _, v in ipairs(cell.values) do
            cells = cells + 1
            collect_timestamp(cell.timestamp)
        end
    else
        cells = cells + 1
        collect_timestamp(cell.timestamp)
    end
end

function collect_row(row)
    for name, cell in pairs(row) do
        rows = rows + 1
        collect_column(cell)
    end
end

-- Consume API hooks

function consume_partition_start(ps)
    partitions = partitions + 1
    if ps.tombstone then
        collect_timestamp(ps.tombstone.timestamp)
    end
end

function consume_static_row(sr)
    collect_row(sr.cells)
end

function consume_clustering_row(cr)
    if cr.marker then
        collect_timestamp(cr.marker.timestamp)
    end

    if cr.tombstone then
        collect_timestamp(cr.tombstone.timestamp)
    end

    collect_row(cr.cells)
end

function consume_range_tombstone_change(crt)
    if crt.tombstone then
        collect_timestamp(crt.tombstone.timestamp)
    end
end

function consume_stream_end()
    print(string.format("Histogram has %d entries, collected from %d partitions, %d rows, %d cells: %d timestamps total", #histogram, partitions, rows, cells, timestamps))

    writer = Scylla.new_json_writer()

    writer:start_object()

    writer:key("buckets")
    writer:start_array()
    for bucket, _ in pairs(histogram) do
        writer:int(bucket)
    end
    writer:end_array()

    writer:key("counts")
    writer:start_array()
    for _, count in pairs(histogram) do
        writer:int(count)
    end
    writer:end_array()

    writer:end_object()
end
