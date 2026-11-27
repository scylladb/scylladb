# Parquet sstables compared with `mt` and dictionary compression

This document compares sstables whose Data file is a Parquet file (format
`pq`, version `aa`, see `sstable-parquet-format.md`) with `mt` sstables
compressed by `ZstdWithDictsCompressor` with a trained dictionary (see
`sstable-compression-dicts.md`). It uses nine public datasets. It measures
the on-disk size, the cost of writing an sstable, and the cost of five kinds
of reads.

Terms used throughout:

- `mt_zstd_dict` is the baseline: an `mt` sstable compressed by
  `ZstdWithDictsCompressor`, with a dictionary trained on the same data.
- `aa_zstd` is the Parquet sstable. Its pages are compressed by ZSTD without
  a dictionary.
- `mt_zstd` is an `mt` sstable compressed by `ZstdCompressor`, without a
  dictionary. It is shown for context only.
- A *ratio* is the value of `aa_zstd` divided by the value of
  `mt_zstd_dict`. A ratio below 1 favours Parquet.
- *CPU* is the CPU time of the reactor thread, user and system time together.
- A *leaf* is a column of the Parquet file. Each CQL column that has a value
  somewhere in the file is one leaf. Each Parquet file here also has two
  metadata leaves, `_scylla.kind` and `_scylla.ts`.
- A *point query* is a query of the partition, slice or row benchmark
  (section 3).

## 1. Summary

Ratios of medians, `aa_zstd` / `mt_zstd_dict`:

| Dataset | Size | Write CPU | Write CPU, net | Full scan CPU | Partition CPU | Slice CPU | Row CPU | Row bytes read | Row I/O ops |
|---|---|---|---|---|---|---|---|---|---|
| drive_stats | 0.70 | 1.03 | 1.05 | 1.07 | 1.46 | 4.96 | 41.34 | 60.36 | 107.42 |
| ghcn_daily | 0.48 | 1.04 | 1.07 | 1.05 | 5.95 | 8.94 | 10.12 | 8.98 | 9.40 |
| fhvhv_trips | 0.62 | 1.14 | 1.21 | 0.96 | 1.04 | 2.46 | 6.40 | 7.03 | 26.94 |
| github_events | 0.66 | 0.71 | 0.66 | 0.66 | 10.64 | 5.25 | 9.18 | 8.66 | 10.22 |
| reviews | 0.86 | 0.85 | 0.80 | 0.92 | 9.84 | 3.74 | 6.17 | 4.35 | 10.28 |
| trades | 0.56 | 1.03 | 1.04 | 0.96 | 0.99 | 3.03 | 4.38 | 2.49 | 9.73 |
| pageviews | 0.77 | 1.00 | 1.00 | 1.11 | 10.82 | 11.15 | 11.07 | 7.95 | 3.74 |
| price_paid | 0.85 | 1.11 | 1.17 | 0.96 | 18.27 | 18.17 | 18.38 | 15.14 | 10.93 |
| isd_lite | 0.32 | 0.99 | 0.98 | 1.00 | 1.06 | 2.94 | 3.78 | 2.73 | 12.06 |

"Write CPU, net" subtracts the cost of scanning the source memtable from
both writes (section 5.2). "Row bytes read" and "Row I/O ops" are the bytes
read and the read requests of single-row queries.

The data supports these conclusions:

- Parquet sstables are smaller on every dataset. They take 0.32 to 0.86 of
  the size of `mt` with a trained dictionary. The gain is largest for
  numeric rows (isd_lite, ghcn_daily, trades) and smallest for rows
  dominated by text (reviews, price_paid).
- Writing costs about the same CPU. Net of the source scan, Parquet costs
  0.66 to 1.21 of `mt_zstd_dict`. It is cheaper for text-heavy rows
  (github_events, reviews) and up to 21% dearer for medium-width
  numeric rows (fhvhv_trips, price_paid).
- A full scan costs about the same CPU: 0.66 to 1.11 of `mt_zstd_dict`.
  Parquet reads fewer bytes. On the other eight datasets, it issues 1.4 to 4.5 times
  more read requests. On isd_lite it issues 7% fewer.
- Point queries are much more expensive with Parquet. A single-row read
  costs 3.8 to 41 times the CPU, reads 2.5 to 60 times the bytes, and issues
  3.7 to 107 times the read requests. Reading a whole partition costs about
  the same when partitions hold thousands of rows (trades, isd_lite,
  fhvhv_trips), and 6 to 18 times more when they hold a few rows.
- The cause is the columnar layout. A point query reads and decodes one
  whole page of every leaf, key and metadata leaves included. `mt` reads one
  compressed block. The cost grows with the number of leaves (section 6.3).
- The dictionary helps `mt` most on github_events, drive_stats and
  price_paid, where `mt_zstd` is 1.4 to 1.8 times larger than
  `mt_zstd_dict`. Even there, Parquet without a dictionary is smaller. On
  isd_lite, the dictionary makes `mt` 3% larger (section 6.2).

## 2. Setup

| Item | Value |
|---|---|
| Commit | `3b2271352c3b134a53377bdce1cce4c1bdb7a88f` (branch `parquet`). The tool `test/perf/perf_sstable_datasets.cc` and the scripts in `test/perf/sstable_datasets/` were uncommitted changes in the working tree. The CPU, byte, and operation numbers were measured before the resource-management rework of the read path and compression, and have not been re-measured. |
| Build | `build/dev/scylla`, dev mode (section 7). |
| CPU | Intel Core i9-14900K: 24 cores (8 performance cores with two threads each, 16 efficiency cores), 32 threads, up to 6.0 GHz, 36 MiB L3 cache. The container saw 30 of the 32 threads. Seastar pins the reactor thread to one CPU. Whether it was a performance or an efficiency core was not recorded. |
| Memory | 64 GiB (65,540,040 kB). |
| Storage | NVMe SSD, Kingston SKC3000D2048G, behind LUKS encryption (dm-crypt). |
| File system | btrfs, mounted with `compress-force=zstd:3`. The kernel compresses every file transparently. |
| Kernel | Linux 7.3.1. Seastar used the io_uring reactor backend. |
| Scylla options | One shard (`-c1`). Memory per dataset: 8 bytes per byte of input, plus 4 GiB, between 4 and 40 GiB (`run_dataset.sh`). `--idle-poll-time-us 0 --poll-aio 0`, so the reactor sleeps when it is idle and its CPU time measures work. |
| Load | The datasets ran one after the other. The machine had no other significant load. The first eight datasets ran from 13:38 to 13:57 on 2026-11-15. isd_lite was added later and ran separately, from 22:44 to 22:46 on the same day, with the same binary and the same settings. |

## 3. Method

The tool is `scylla perf-sstable-datasets` (`test/perf/perf_sstable_datasets.cc`).
`test/perf/sstable_datasets/run_dataset.sh` runs it on one dataset. For each
dataset, it does the following on one shard:

1. It loads `data.jsonl` into one memtable. Each line is one row, inserted as
   by `INSERT ... JSON`. Each row gets a row marker. All cells of a row have
   the same write timestamp: a fixed base plus the line number. This models
   rows inserted in file order. There are no TTLs and no tombstones.
2. It scans the memtable once to collect the keys and a content digest.
3. It scans the memtable three more times and measures each scan. This is
   the *source* cost. Every write below reads the same memtable, so the
   source cost is part of every write cost.
4. It writes the memtable to `mt_zstd` three times and keeps the last
   sstable.
5. It trains a dictionary as production does
   (`replica::database::sample_data_files()` and
   `dictionary_service::train_dict()`): 4096 samples of 4096 bytes each,
   taken from the uncompressed Data file of `mt_zstd` at aligned offsets,
   chosen uniformly with replacement. `zdict_train()` builds a dictionary of
   at most 110 KiB from them. The tool makes it the recommended dictionary of
   the table.
6. It writes the memtable to `mt_zstd_dict` and then to `aa_zstd`, three
   times each.
7. It reads the head of `CompressionInfo.db` of both `mt` sstables. It checks
   that `mt_zstd_dict` holds exactly the trained dictionary and that
   `mt_zstd` holds none. It records the size of every component file.
8. It checks correctness. A full scan of every sstable must give the
   memtable's digest, partition count and row count. Each read benchmark
   must give the same digest in all three configurations.
9. It runs the read benchmarks three times. Each run takes the benchmarks in
   turn, and for each benchmark the three configurations in turn.

The configurations use the defaults of a table that sets only the compressor
class:

| Parameter | Value |
|---|---|
| `mt` chunk length | 4096 bytes |
| ZSTD level | 3 (both formats) |
| Dictionary | 4096 samples of 4096 bytes, at most 110 KiB (112,640 bytes); trained on the dataset itself |
| Parquet row group target | 8 MiB (`parquet_row_group_size_in_kb`); the memory budget of 5% of the shard's memory was larger in every run |
| Parquet page set size | 128 KiB (`parquet_page_set_size_in_kb`) |
| Parquet minimum page size | 8 KiB of raw values (`parquet_min_page_size_in_kb`) |
| Parquet dictionary page limit | 4 KiB (`parquet_dictionary_page_size_limit_in_kb`) |
| Parquet compression codec | ZSTD at level 3, with a 128 KiB window, without a dictionary |

The `mt` index is the trie index (`Partitions.db`, `Rows.db`) with a row
index block every 64 KiB of uncompressed data (`column_index_size_in_kb`).
The `aa` sstable uses the same trie index. Its row index blocks end at page
set boundaries.

The read benchmarks:

| Benchmark | What one query reads |
|---|---|
| Open | Opening the sstable (`sstable::load()`), as a node opens it. Measured before each read benchmark, so 12 times per configuration. |
| Full scan | `make_full_scan_reader()` over the whole sstable. One query per run. |
| Partition | A whole partition, chosen uniformly among the partitions. |
| Slice | A clustering range of min(100, max(1, rows / 10)) consecutive rows of one partition. The partition is chosen with probability proportional to its rows. |
| Row | One row by its full primary key, chosen uniformly among the rows. |

Partition, slice and row each run 1000 queries, the same queries in every
configuration and run (seed 1). Each query uses its own reader.

What is cold and what is warm:

- Each (run, benchmark, configuration) opens the sstable anew. So the
  sstable's own caches start empty: the cached pages of `Partitions.db` and
  `Rows.db`, and for `aa`, the cache of OffsetIndexes and dictionary
  pages.
- Within the 1000 queries of a benchmark, these caches warm up. The first
  queries read index pages and OffsetIndexes. Later queries find most of
  them in the cache.
- No configuration caches Data file contents between queries. There is no
  row cache.
- The operating system's page cache is not dropped. The I/O counts below
  are requests that Seastar issued, not requests that reached the device.

Measures:

- *CPU* is `CLOCK_THREAD_CPUTIME_ID` of the reactor thread. It includes
  system time spent in that thread, for example for I/O submission.
- *Instructions* are user-mode instructions retired by the reactor thread
  (a perf event). They exclude kernel work.
- *Read bytes* and *read ops* are Seastar's counters of reads issued
  (`aio_read_bytes`, `aio_reads`). A read covers whole 4 KiB blocks. Writes
  are counted the same way.
- *Wall* is elapsed time. It depends on the file system and the page cache
  (section 7), so the analysis does not use it.
- Each value is the median of three runs (open: of 12).
- Per-query values divide the median by 1000.

## 4. Datasets

Each dataset is a subset of a public source of about 400 MB of JSON lines.
`test/perf/sstable_datasets/conv_<name>.py` converts it. The subsets:

| Dataset | Subset | Workload shape |
|---|---|---|
| drive_stats | Backblaze Drive Stats, Q1 2024. Whole days from 2024-01-01, of the vaults whose crc32(vault_id) falls in a fixed 40% subset. | Wide, sparse rows: daily SMART snapshots of disk drives. A partition is (day, cluster, vault), with about 1200 drives sorted by pod, slot and serial number. |
| ghcn_daily | NOAA GHCN-Daily, 2023 file. The first lines, which cover all stations for the first days of 2023. | Narrow numeric time series. A partition is a weather station, clustered by (date, element). |
| fhvhv_trips | NYC TLC high-volume for-hire vehicle trips, January 2024. The first row group of the source file. | Medium-width numeric rows. A partition is (pickup zone, pickup day), clustered by pickup time. |
| github_events | GH Archive, the hour 2024-01-15 15:00 UTC. All events of the hour. | Semi-structured events with a large JSON text payload. A partition is a repository, usually with one event. |
| reviews | Amazon Reviews 2023, Software category. A prefix of the file. | Text-heavy rows. A partition is a product, clustered newest first. Partition sizes are skewed. |
| trades | Binance spot BTCUSDT trades, 2024-01-15 and 2024-01-16. Whole hours from the start. | Narrow fixed-width numbers. A partition is (symbol, hour), with tens of thousands of rows clustered by an increasing trade id. |
| pageviews | Wikimedia pageviews, 2024-01-15, hours 15 to 18 UTC. The (domain, title) pairs whose crc32 falls in a fixed 11.5% subset. | One int per row behind a long text partition key. One to four rows per partition, one per hour. |
| price_paid | HM Land Registry Price Paid Data, 2023 and the start of 2022. Rows with an empty postcode are dropped. | Address-like text columns with low-cardinality codes. A partition is a postcode, clustered by date and transaction uuid. |
| isd_lite | NOAA ISD-Lite, 2023, US stations only. The candidates are the first 600 stations with country `US` in `isd-history.csv` whose period of record covers 2023, in the order of crc32(station_id). Whole stations with at least 8,322 hourly rows (95% of the year) are kept, in that order, until the size target is reached. | Numeric hourly time series. A partition is a weather station, with 8,327 to 8,760 rows clustered by observation hour. Eight int columns. |

The sources and their licences, as `meta.json` records them:

| Dataset | Source | Licence (as recorded by the converter) |
|---|---|---|
| drive_stats | https://f001.backblazeb2.com/file/Backblaze-Hard-Drive-Data/data_Q1_2024.zip | Backblaze Drive Stats: free to use with attribution to Backblaze; may not be sold (https://www.backblaze.com/cloud-storage/resources/hard-drive-test-data) |
| ghcn_daily | https://www.ncei.noaa.gov/pub/data/ghcn/daily/by_year/2023.csv.gz<br>https://noaa-ghcn-pds.s3.amazonaws.com/csv.gz/by_year/2023.csv.gz (mirror) | NOAA public data, no restrictions (U.S. Government work; cite GHCN-Daily, Menne et al.) |
| fhvhv_trips | https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2024-01.parquet | NYC TLC trip record data, NYC Open Data terms of use (public, no restrictions) |
| github_events | https://data.gharchive.org/2024-01-15-15.json.gz | GH Archive metadata: CC0-like public data (gharchive.org); event contents subject to GitHub terms |
| reviews | https://mcauleylab.ucsd.edu/public_datasets/data/amazon_2023/raw/review_categories/Software.jsonl.gz | Amazon Reviews 2023 (McAuley Lab, UCSD): research use; no explicit open licence |
| trades | https://data.binance.vision/data/spot/daily/trades/BTCUSDT/BTCUSDT-trades-2024-01-15.zip<br>https://data.binance.vision/data/spot/daily/trades/BTCUSDT/BTCUSDT-trades-2024-01-16.zip | Binance public market data (data.binance.vision); terms of use, no explicit open licence |
| pageviews | https://dumps.wikimedia.org/other/pageviews/2024/2024-01/pageviews-20240115-150000.gz<br>https://dumps.wikimedia.org/other/pageviews/2024/2024-01/pageviews-20240115-160000.gz<br>https://dumps.wikimedia.org/other/pageviews/2024/2024-01/pageviews-20240115-170000.gz<br>https://dumps.wikimedia.org/other/pageviews/2024/2024-01/pageviews-20240115-180000.gz | Wikimedia pageviews: CC0 1.0 |
| price_paid | https://price-paid-data.publicdata.landregistry.gov.uk/pp-2023.csv<br>https://price-paid-data.publicdata.landregistry.gov.uk/pp-2022.csv | Open Government Licence v3.0 (contains HM Land Registry data, Crown copyright) |
| isd_lite | https://noaa-isd-pds.s3.amazonaws.com/isd-history.csv<br>https://noaa-isd-pds.s3.amazonaws.com/isd-lite/data/2023/<station_id>-2023.gz | NOAA open data (NODD): open to the public, attribution requested, no implied endorsement (registry.opendata.aws/noaa-isd); cite NOAA NCEI (2001), Global Surface Hourly. US stations only: the non-US data in ISD are subject to WMO Resolution 40. |

Note: the GH Archive entry describes the archive's own data. GH Archive
publishes no explicit licence.

Notes on isd_lite:

- The NCEI paths `https://www.ncei.noaa.gov/pub/data/noaa/isd-lite/...`
  now redirect to the NOAA bucket `noaa-isd-pds` on Amazon S3. The
  converter uses the bucket directly. The format is described in
  `isd-lite/isd-lite-format.txt` in that bucket.
- The licence entry combines two sources. The AWS Open Data Registry entry
  for ISD gives the NOAA Open Data Dissemination terms: the data "are open
  to the public and can be used as desired", with attribution requested. The
  bucket's `readme.txt` states that the non-US data in ISD are subject to
  WMO Resolution 40 and cannot be redistributed. So the subset uses US
  stations only. Neither source uses the term "public domain".
- The values keep the integer encoding of ISD-Lite. Air temperature and dew
  point are tenths of a degree Celsius. Sea level pressure is tenths of a
  hectopascal. Wind speed is tenths of a metre per second. Precipitation
  over 1 and 6 hours is tenths of a millimetre, and -1 means a trace. Wind
  direction is in degrees, with 0 for calm. Sky condition is a code from 0
  to 19. The missing value -9999 becomes an absent cell. Observation hours
  are UTC.
- The cells per row vary. Air temperature is present in 98% of the rows,
  sea level pressure in 61%, sky condition in 50%, 1-hour precipitation in
  50% and 6-hour precipitation in 2%.

The sizes of the datasets. "Input JSONL" is the size of `data.jsonl`. "mt
uncompressed Data" is the uncompressed length of the `mt` Data file.

| Dataset | Rows | Partitions | Rows per partition, median | Rows per partition, max | Input JSONL (MiB) | Input bytes per row | Cells | mt uncompressed Data (MiB) |
|---|---|---|---|---|---|---|---|---|
| drive_stats | 357,273 | 321 | 1,220 | 1,220 | 420.2 | 1,233 | 17,743,539 | 152.0 |
| ghcn_daily | 3,894,438 | 38,070 | 71 | 801 | 381.5 | 103 | 10,005,195 | 110.9 |
| fhvhv_trips | 632,741 | 258 | 1,867 | 14,528 | 381.5 | 632 | 13,527,292 | 109.4 |
| github_events | 108,565 | 42,319 | 1 | 1,275 | 381.5 | 3,684 | 680,104 | 345.2 |
| reviews | 1,019,230 | 54,825 | 2 | 9,548 | 381.5 | 392 | 7,134,610 | 238.1 |
| trades | 1,857,521 | 28 | 51,073 | 302,218 | 390.9 | 221 | 11,145,126 | 97.4 |
| pageviews | 3,788,176 | 2,189,198 | 1 | 4 | 375.0 | 104 | 3,788,176 | 170.9 |
| price_paid | 1,212,238 | 605,110 | 1 | 259 | 381.5 | 330 | 13,941,240 | 150.6 |
| isd_lite | 2,477,725 | 286 | 8,699 | 8,760 | 381.7 | 162 | 13,079,712 | 100.6 |

The CQL schemas follow. The tool replaces the compression options.

`drive_stats`:

```cql
CREATE TABLE bench.drive_stats (
  date date,
  cluster_id int,
  vault_id int,
  pod_id int,
  pod_slot_num int,
  serial_number text,
  model text,
  capacity_bytes bigint,
  failure boolean,
  datacenter text,
  is_legacy_format boolean,
  -- 182 SMART columns: smart_<N>_normalized int and smart_<N>_raw bigint
  -- for the 91 attributes N = 1, 2, 3, 4, 5, 7, 8, 9, 10, 11, 12, 13, 15, 16, 17, 18, 22, 23, 24, 27, 71, 82, 90, 160, 161, 163, 164, 165, 166, 167, 168, 169, 170, 171, 172, 173, 174, 175, 176, 177, 178, 179, 180, 181, 182, 183, 184, 187, 188, 189, 190, 191, 192, 193, 194, 195, 196, 197, 198, 199, 200, 201, 202, 206, 210, 218, 220, 222, 223, 224, 225, 226, 230, 231, 232, 233, 234, 235, 240, 241, 242, 244, 245, 246, 247, 248, 250, 251, 252, 254, 255.
  PRIMARY KEY ((date, cluster_id, vault_id), pod_id, pod_slot_num, serial_number)
);
```

`ghcn_daily`:

```cql
CREATE TABLE bench.ghcn_daily (
  station_id text,
  obs_date date,
  element text,
  value int,
  mflag text,
  qflag text,
  sflag text,
  obs_time text,
  PRIMARY KEY ((station_id), obs_date, element)
);
```

`fhvhv_trips`:

```cql
CREATE TABLE bench.fhvhv_trips (
  pu_location_id int,
  pickup_date date,
  pickup_datetime timestamp,
  seq bigint,
  hvfhs_license_num text,
  dispatching_base_num text,
  originating_base_num text,
  request_datetime timestamp,
  on_scene_datetime timestamp,
  dropoff_datetime timestamp,
  do_location_id int,
  trip_miles double,
  trip_time bigint,
  base_passenger_fare double,
  tolls double,
  bcf double,
  sales_tax double,
  congestion_surcharge double,
  airport_fee double,
  tips double,
  driver_pay double,
  shared_request_flag text,
  shared_match_flag text,
  access_a_ride_flag text,
  wav_request_flag text,
  wav_match_flag text,
  PRIMARY KEY ((pu_location_id, pickup_date), pickup_datetime, seq)
);
```

`github_events`:

```cql
CREATE TABLE bench.github_events (
  repo_id bigint,
  created_at timestamp,
  event_id bigint,
  event_type text,
  repo_name text,
  actor_id bigint,
  actor_login text,
  org_login text,
  is_public boolean,
  payload text,
  PRIMARY KEY ((repo_id), created_at, event_id)
);
```

`reviews`:

```cql
CREATE TABLE bench.reviews (
  parent_asin text,
  review_time timestamp,
  user_id text,
  asin text,
  rating float,
  title text,
  body text,
  images_json text,
  helpful_vote int,
  verified_purchase boolean,
  PRIMARY KEY ((parent_asin), review_time, user_id)
) WITH CLUSTERING ORDER BY (review_time DESC, user_id ASC);
```

`trades`:

```cql
CREATE TABLE bench.trades (
  symbol text,
  hour_bucket timestamp,
  trade_id bigint,
  price double,
  qty double,
  quote_qty double,
  trade_time timestamp,
  is_buyer_maker boolean,
  is_best_match boolean,
  PRIMARY KEY ((symbol, hour_bucket), trade_id)
);
```

`pageviews`:

```cql
CREATE TABLE bench.pageviews (
  domain_code text,
  page_title text,
  hour timestamp,
  views int,
  PRIMARY KEY ((domain_code, page_title), hour)
);
```

`price_paid`:

```cql
CREATE TABLE bench.price_paid (
  postcode text,
  transfer_date date,
  transaction_id uuid,
  price bigint,
  property_type text,
  old_new text,
  duration text,
  paon text,
  saon text,
  street text,
  locality text,
  town text,
  district text,
  county text,
  ppd_category text,
  record_status text,
  PRIMARY KEY ((postcode), transfer_date, transaction_id)
);
```

`isd_lite`:

```cql
CREATE TABLE bench.isd_lite (
  station_id text,
  obs_time timestamp,
  air_temp int,
  dew_point int,
  sea_level_pressure int,
  wind_direction int,
  wind_speed int,
  sky_condition int,
  precip_1h int,
  precip_6h int,
  PRIMARY KEY ((station_id), obs_time)
);
```

## 5. Results

All values are medians of three runs. All tables come from
`results_tables.py` (section 8).

### 5.1 Size

"Index" is `Partitions.db` and `Rows.db`. "Other" is `Statistics.db`,
`Scylla.db`, `TOC.txt` and `Digest.crc32`. `CompressionInfo.db` of
`mt_zstd_dict` holds the dictionary, 112,697 bytes with its framing. A
Parquet sstable has no `CompressionInfo.db`. Its Data file holds its own
page offsets.

| Dataset | Config | Total (MiB) | Data.db (MiB) | Index (KiB) | Filter (KiB) | CompressionInfo (KiB) | Other (KiB) | Total / mt uncompressed | Total / input | Total / mt_zstd_dict |
|---|---|---|---|---|---|---|---|---|---|---|
| drive_stats | mt_zstd | 21.67 | 21.30 | 54 | 0 | 304 | 28 | 0.143 | 0.052 | 1.39 |
| drive_stats | mt_zstd_dict | 15.63 | 15.14 | 54 | 0 | 414 | 28 | 0.103 | 0.037 | 1.00 |
| drive_stats | aa_zstd | 10.93 | 10.87 | 30 | 0 | 0 | 28 | 0.072 | 0.026 | 0.70 |
| ghcn_daily | mt_zstd | 30.38 | 29.75 | 366 | 46 | 222 | 6 | 0.274 | 0.080 | 1.05 |
| ghcn_daily | mt_zstd_dict | 28.83 | 28.10 | 366 | 46 | 332 | 6 | 0.260 | 0.076 | 1.00 |
| ghcn_daily | aa_zstd | 13.70 | 13.29 | 373 | 46 | 0 | 6 | 0.124 | 0.036 | 0.48 |
| fhvhv_trips | mt_zstd | 40.05 | 39.80 | 27 | 0 | 219 | 8 | 0.366 | 0.105 | 1.25 |
| fhvhv_trips | mt_zstd_dict | 32.00 | 31.65 | 27 | 0 | 329 | 8 | 0.292 | 0.084 | 1.00 |
| fhvhv_trips | aa_zstd | 19.97 | 19.95 | 17 | 0 | 0 | 8 | 0.183 | 0.052 | 0.62 |
| github_events | mt_zstd | 98.76 | 97.51 | 533 | 52 | 690 | 6 | 0.286 | 0.259 | 1.81 |
| github_events | mt_zstd_dict | 54.60 | 53.25 | 533 | 52 | 801 | 6 | 0.158 | 0.143 | 1.00 |
| github_events | aa_zstd | 35.99 | 35.53 | 420 | 52 | 0 | 6 | 0.104 | 0.094 | 0.66 |
| reviews | mt_zstd | 138.11 | 137.04 | 546 | 67 | 476 | 6 | 0.580 | 0.362 | 1.25 |
| reviews | mt_zstd_dict | 110.16 | 108.98 | 546 | 67 | 586 | 6 | 0.463 | 0.289 | 1.00 |
| reviews | aa_zstd | 94.78 | 94.15 | 565 | 67 | 0 | 6 | 0.398 | 0.248 | 0.86 |
| trades | mt_zstd | 34.24 | 34.03 | 15 | 0 | 195 | 6 | 0.352 | 0.088 | 1.04 |
| trades | mt_zstd_dict | 33.05 | 32.73 | 15 | 0 | 305 | 6 | 0.339 | 0.085 | 1.00 |
| trades | aa_zstd | 18.66 | 18.64 | 7 | 0 | 0 | 6 | 0.192 | 0.048 | 0.56 |
| pageviews | mt_zstd | 107.96 | 87.21 | 18,223 | 2,672 | 342 | 6 | 0.632 | 0.288 | 1.16 |
| pageviews | mt_zstd_dict | 92.68 | 71.83 | 18,223 | 2,672 | 452 | 6 | 0.542 | 0.247 | 1.00 |
| pageviews | aa_zstd | 71.40 | 53.16 | 15,994 | 2,672 | 0 | 6 | 0.418 | 0.190 | 0.77 |
| price_paid | mt_zstd | 70.57 | 64.59 | 5,075 | 739 | 301 | 7 | 0.469 | 0.185 | 1.37 |
| price_paid | mt_zstd_dict | 51.63 | 45.55 | 5,075 | 739 | 411 | 7 | 0.343 | 0.135 | 1.00 |
| price_paid | aa_zstd | 43.82 | 38.68 | 4,515 | 739 | 0 | 7 | 0.291 | 0.115 | 0.85 |
| isd_lite | mt_zstd | 30.30 | 30.07 | 27 | 0 | 201 | 6 | 0.301 | 0.079 | 0.97 |
| isd_lite | mt_zstd_dict | 31.32 | 30.98 | 27 | 0 | 311 | 6 | 0.311 | 0.082 | 1.00 |
| isd_lite | aa_zstd | 9.94 | 9.92 | 19 | 0 | 0 | 6 | 0.099 | 0.026 | 0.32 |

The dictionaries. Every `mt_zstd_dict` sstable holds exactly the trained
dictionary, as the tool checks.

| Dataset | Dictionary (B) | Samples | Sample size (B) | Chunks in mt_zstd Data | Samples / chunks | Same dictionary in mt_zstd_dict | CompressionInfo.db growth (B) |
|---|---|---|---|---|---|---|---|
| drive_stats | 112,640 | 4,096 | 4,096 | 38,924 | 0.11 | yes | 112,697 |
| ghcn_daily | 112,640 | 4,096 | 4,096 | 28,387 | 0.14 | yes | 112,697 |
| fhvhv_trips | 112,640 | 4,096 | 4,096 | 28,010 | 0.15 | yes | 112,697 |
| github_events | 112,640 | 4,096 | 4,096 | 88,371 | 0.05 | yes | 112,697 |
| reviews | 112,640 | 4,096 | 4,096 | 60,960 | 0.07 | yes | 112,697 |
| trades | 112,640 | 4,096 | 4,096 | 24,938 | 0.16 | yes | 112,697 |
| pageviews | 112,640 | 4,096 | 4,096 | 43,748 | 0.09 | yes | 112,697 |
| price_paid | 112,640 | 4,096 | 4,096 | 38,552 | 0.11 | yes | 112,697 |
| isd_lite | 112,640 | 4,096 | 4,096 | 25,749 | 0.16 | yes | 112,697 |

### 5.2 Write

One write of the whole memtable to one sstable. "Source scan" is the cost of
reading the memtable alone, which every write includes. "Net" subtracts it.

| Dataset | Config | Wall (s) | CPU (s) | CPU net (s) | CPU net per row (ns) | Instructions (G) | Instructions net (G) | Written (MiB) | Write ops | CPU / dict | CPU net / dict |
|---|---|---|---|---|---|---|---|---|---|---|---|
| drive_stats | mt_zstd | 1.84 | 1.81 | 1.33 | 3,720 | 35.2 | 27.2 | 21.7 | 182 | 1.08 | 1.12 |
| drive_stats | mt_zstd_dict | 1.70 | 1.67 | 1.19 | 3,333 | 33.4 | 25.4 | 15.7 | 134 | 1.00 | 1.00 |
| drive_stats | aa_zstd | 1.79 | 1.73 | 1.25 | 3,487 | 31.7 | 23.7 | 11.0 | 95 | 1.03 | 1.05 |
| drive_stats | source scan | 0.48 | 0.48 |  |  | 8.0 |  |  |  |  |  |
| ghcn_daily | mt_zstd | 2.40 | 2.36 | 1.43 | 368 | 40.0 | 26.7 | 31.0 | 268 | 1.02 | 1.04 |
| ghcn_daily | mt_zstd_dict | 2.35 | 2.30 | 1.38 | 354 | 38.9 | 25.7 | 29.4 | 255 | 1.00 | 1.00 |
| ghcn_daily | aa_zstd | 2.43 | 2.40 | 1.47 | 378 | 42.1 | 28.9 | 14.3 | 135 | 1.04 | 1.07 |
| ghcn_daily | source scan | 0.93 | 0.93 |  |  | 13.3 |  |  |  |  |  |
| fhvhv_trips | mt_zstd | 1.47 | 1.44 | 1.00 | 1,575 | 24.9 | 17.9 | 40.1 | 329 | 1.05 | 1.08 |
| fhvhv_trips | mt_zstd_dict | 1.40 | 1.36 | 0.92 | 1,459 | 23.4 | 16.3 | 32.0 | 265 | 1.00 | 1.00 |
| fhvhv_trips | aa_zstd | 1.63 | 1.56 | 1.12 | 1,765 | 28.3 | 21.3 | 20.0 | 168 | 1.14 | 1.21 |
| fhvhv_trips | source scan | 0.44 | 0.44 |  |  | 7.0 |  |  |  |  |  |
| github_events | mt_zstd | 1.09 | 1.03 | 0.95 | 8,744 | 16.2 | 15.2 | 99.4 | 818 | 1.65 | 1.75 |
| github_events | mt_zstd_dict | 0.67 | 0.63 | 0.54 | 5,006 | 9.3 | 8.4 | 55.3 | 464 | 1.00 | 1.00 |
| github_events | aa_zstd | 0.48 | 0.44 | 0.36 | 3,312 | 6.6 | 5.7 | 36.7 | 315 | 0.71 | 0.66 |
| github_events | source scan | 0.09 | 0.09 |  |  | 0.9 |  |  |  |  |  |
| reviews | mt_zstd | 1.96 | 1.91 | 1.45 | 1,424 | 31.7 | 25.6 | 139.0 | 1,138 | 0.95 | 0.93 |
| reviews | mt_zstd_dict | 2.07 | 2.02 | 1.56 | 1,529 | 30.0 | 23.8 | 111.0 | 914 | 1.00 | 1.00 |
| reviews | aa_zstd | 1.77 | 1.71 | 1.25 | 1,228 | 27.5 | 21.4 | 95.6 | 791 | 0.85 | 0.80 |
| reviews | source scan | 0.46 | 0.46 |  |  | 6.1 |  |  |  |  |  |
| trades | mt_zstd | 1.36 | 1.33 | 0.83 | 446 | 25.4 | 16.5 | 34.3 | 283 | 1.00 | 1.00 |
| trades | mt_zstd_dict | 1.36 | 1.33 | 0.83 | 446 | 24.9 | 15.9 | 33.1 | 273 | 1.00 | 1.00 |
| trades | aa_zstd | 1.43 | 1.37 | 0.86 | 466 | 26.8 | 17.9 | 18.7 | 158 | 1.03 | 1.04 |
| trades | source scan | 0.51 | 0.50 |  |  | 9.0 |  |  |  |  |  |
| pageviews | mt_zstd | 4.09 | 3.98 | 2.04 | 538 | 58.6 | 32.9 | 141.4 | 1,938 | 0.97 | 0.94 |
| pageviews | mt_zstd_dict | 4.21 | 4.10 | 2.16 | 570 | 58.4 | 32.8 | 126.1 | 1,816 | 1.00 | 1.00 |
| pageviews | aa_zstd | 4.23 | 4.11 | 2.17 | 572 | 58.7 | 33.0 | 104.8 | 1,646 | 1.00 | 1.00 |
| pageviews | source scan | 1.95 | 1.94 |  |  | 25.7 |  |  |  |  |  |
| price_paid | mt_zstd | 2.60 | 2.53 | 1.60 | 1,316 | 39.4 | 26.8 | 79.8 | 866 | 1.04 | 1.07 |
| price_paid | mt_zstd_dict | 2.47 | 2.43 | 1.49 | 1,232 | 36.7 | 24.1 | 60.9 | 715 | 1.00 | 1.00 |
| price_paid | aa_zstd | 2.73 | 2.69 | 1.75 | 1,447 | 42.7 | 30.2 | 53.1 | 653 | 1.11 | 1.17 |
| price_paid | source scan | 0.94 | 0.93 |  |  | 12.6 |  |  |  |  |  |
| isd_lite | mt_zstd | 1.63 | 1.59 | 1.10 | 442 | 30.9 | 21.6 | 30.3 | 251 | 1.02 | 1.03 |
| isd_lite | mt_zstd_dict | 1.58 | 1.56 | 1.06 | 429 | 30.2 | 20.9 | 31.4 | 259 | 1.00 | 1.00 |
| isd_lite | aa_zstd | 1.59 | 1.54 | 1.04 | 420 | 30.5 | 21.2 | 10.0 | 88 | 0.99 | 0.98 |
| isd_lite | source scan | 0.50 | 0.49 |  |  | 9.3 |  |  |  |  |  |

### 5.3 Open

Opening the sstable, per open. The read bytes include the whole
`CompressionInfo.db` for `mt` and the whole `Filter.db` for both formats.

| Dataset | mt_zstd CPU (us) | mt_zstd KiB | mt_zstd ops | mt_zstd_dict CPU (us) | mt_zstd_dict KiB | mt_zstd_dict ops | aa_zstd CPU (us) | aa_zstd KiB | aa_zstd ops |
|---|---|---|---|---|---|---|---|---|---|
| drive_stats | 562 | 476 | 18 | 679 | 584 | 19 | 537 | 204 | 17 |
| ghcn_daily | 452 | 364 | 17 | 565 | 472 | 18 | 276 | 148 | 17 |
| fhvhv_trips | 448 | 316 | 17 | 578 | 428 | 18 | 333 | 108 | 17 |
| github_events | 770 | 836 | 21 | 878 | 948 | 22 | 280 | 152 | 17 |
| reviews | 614 | 640 | 19 | 734 | 748 | 20 | 309 | 176 | 17 |
| trades | 438 | 292 | 17 | 552 | 404 | 18 | 307 | 104 | 17 |
| pageviews | 1,232 | 3,112 | 38 | 1,311 | 3,220 | 39 | 965 | 2,780 | 37 |
| price_paid | 676 | 1,136 | 23 | 784 | 1,244 | 24 | 489 | 844 | 22 |
| isd_lite | 447 | 300 | 17 | 585 | 408 | 18 | 326 | 108 | 17 |

### 5.4 Full scan

| Dataset | Config | CPU (s) | CPU per row (ns) | Instructions per row | Read (MiB) | Read ops | CPU / dict | Bytes / dict |
|---|---|---|---|---|---|---|---|---|
| drive_stats | mt_zstd | 1.46 | 4,088 | 83,367 | 21.3 | 179 | 1.00 | 1.41 |
| drive_stats | mt_zstd_dict | 1.46 | 4,096 | 82,442 | 15.1 | 130 | 1.00 | 1.00 |
| drive_stats | aa_zstd | 1.57 | 4,403 | 86,426 | 12.8 | 585 | 1.07 | 0.84 |
| ghcn_daily | mt_zstd | 1.42 | 365 | 7,373 | 29.8 | 247 | 0.98 | 1.06 |
| ghcn_daily | mt_zstd_dict | 1.45 | 372 | 7,339 | 28.1 | 233 | 1.00 | 1.00 |
| ghcn_daily | aa_zstd | 1.52 | 390 | 8,181 | 14.1 | 318 | 1.05 | 0.50 |
| fhvhv_trips | mt_zstd | 1.24 | 1,963 | 37,280 | 39.8 | 327 | 1.00 | 1.26 |
| fhvhv_trips | mt_zstd_dict | 1.24 | 1,966 | 36,228 | 31.6 | 262 | 1.00 | 1.00 |
| fhvhv_trips | aa_zstd | 1.19 | 1,880 | 38,915 | 21.3 | 495 | 0.96 | 0.67 |
| github_events | mt_zstd | 0.40 | 3,686 | 62,220 | 97.5 | 789 | 1.41 | 1.83 |
| github_events | mt_zstd_dict | 0.28 | 2,611 | 35,945 | 53.2 | 434 | 1.00 | 1.00 |
| github_events | aa_zstd | 0.19 | 1,735 | 29,333 | 37.5 | 796 | 0.66 | 0.70 |
| reviews | mt_zstd | 1.00 | 986 | 18,583 | 137.0 | 1,105 | 1.02 | 1.26 |
| reviews | mt_zstd_dict | 0.98 | 965 | 17,169 | 109.0 | 880 | 1.00 | 1.00 |
| reviews | aa_zstd | 0.90 | 887 | 18,381 | 99.5 | 2,123 | 0.92 | 0.91 |
| trades | mt_zstd | 1.13 | 610 | 11,850 | 34.0 | 281 | 0.98 | 1.04 |
| trades | mt_zstd_dict | 1.15 | 620 | 11,886 | 32.7 | 270 | 1.00 | 1.00 |
| trades | aa_zstd | 1.10 | 595 | 12,951 | 19.8 | 441 | 0.96 | 0.60 |
| pageviews | mt_zstd | 1.74 | 459 | 7,842 | 87.2 | 706 | 1.01 | 1.21 |
| pageviews | mt_zstd_dict | 1.72 | 454 | 7,594 | 71.8 | 583 | 1.00 | 1.00 |
| pageviews | aa_zstd | 1.90 | 502 | 9,244 | 56.4 | 1,242 | 1.11 | 0.78 |
| price_paid | mt_zstd | 1.67 | 1,375 | 24,082 | 64.6 | 525 | 1.01 | 1.42 |
| price_paid | mt_zstd_dict | 1.65 | 1,359 | 23,234 | 45.6 | 373 | 1.00 | 1.00 |
| price_paid | aa_zstd | 1.59 | 1,311 | 25,492 | 41.1 | 924 | 0.96 | 0.90 |
| isd_lite | mt_zstd | 1.30 | 525 | 10,450 | 30.1 | 249 | 0.98 | 0.97 |
| isd_lite | mt_zstd_dict | 1.33 | 537 | 10,376 | 31.0 | 256 | 1.00 | 1.00 |
| isd_lite | aa_zstd | 1.33 | 536 | 11,276 | 10.5 | 237 | 1.00 | 0.34 |

### 5.5 Point queries

Per query, averaged over the 1000 queries of a run.

Whole partition:

| Dataset | Config | Rows per query | CPU per query (us) | Instructions per query (M) | KiB read per query | Read ops per query | CPU / dict | Bytes / dict | Ops / dict |
|---|---|---|---|---|---|---|---|---|---|
| drive_stats | mt_zstd | 1,125 | 4,710.6 | 94.55 | 73.3 | 1.0 | 1.00 | 1.38 | 1.00 |
| drive_stats | mt_zstd_dict | 1,125 | 4,726.7 | 93.50 | 53.0 | 1.0 | 1.00 | 1.00 | 1.00 |
| drive_stats | aa_zstd | 1,125 | 6,910.8 | 132.36 | 974.2 | 175.1 | 1.46 | 18.38 | 171.71 |
| ghcn_daily | mt_zstd | 99 | 49.0 | 0.83 | 6.3 | 1.1 | 0.95 | 1.02 | 1.00 |
| ghcn_daily | mt_zstd_dict | 99 | 51.5 | 0.82 | 6.1 | 1.1 | 1.00 | 1.00 | 1.00 |
| ghcn_daily | aa_zstd | 99 | 306.4 | 6.06 | 62.4 | 10.4 | 5.95 | 10.18 | 9.55 |
| fhvhv_trips | mt_zstd | 2,479 | 4,899.2 | 92.70 | 165.3 | 1.8 | 0.99 | 1.26 | 1.13 |
| fhvhv_trips | mt_zstd_dict | 2,479 | 4,926.6 | 90.07 | 131.7 | 1.6 | 1.00 | 1.00 | 1.00 |
| fhvhv_trips | aa_zstd | 2,479 | 5,136.1 | 101.97 | 422.5 | 63.5 | 1.04 | 3.21 | 38.98 |
| github_events | mt_zstd | 3 | 22.8 | 0.24 | 8.1 | 1.1 | 1.18 | 1.22 | 1.00 |
| github_events | mt_zstd_dict | 3 | 19.3 | 0.15 | 6.6 | 1.1 | 1.00 | 1.00 | 1.00 |
| github_events | aa_zstd | 3 | 205.2 | 3.31 | 85.6 | 11.7 | 10.64 | 12.91 | 10.32 |
| reviews | mt_zstd | 16 | 30.5 | 0.42 | 9.2 | 1.1 | 1.03 | 1.10 | 1.00 |
| reviews | mt_zstd_dict | 16 | 29.7 | 0.37 | 8.3 | 1.1 | 1.00 | 1.00 | 1.00 |
| reviews | aa_zstd | 16 | 292.5 | 5.69 | 116.7 | 11.9 | 9.84 | 14.01 | 10.48 |
| trades | mt_zstd | 68,131 | 40,824.9 | 809.46 | 1,276.4 | 10.5 | 0.98 | 1.04 | 1.04 |
| trades | mt_zstd_dict | 68,131 | 41,458.3 | 811.81 | 1,226.0 | 10.0 | 1.00 | 1.00 | 1.00 |
| trades | aa_zstd | 68,131 | 41,015.6 | 887.19 | 1,174.4 | 52.9 | 0.99 | 0.96 | 5.28 |
| pageviews | mt_zstd | 2 | 21.7 | 0.14 | 9.9 | 2.0 | 1.04 | 1.04 | 1.00 |
| pageviews | mt_zstd_dict | 2 | 20.8 | 0.12 | 9.5 | 2.0 | 1.00 | 1.00 | 1.00 |
| pageviews | aa_zstd | 2 | 225.2 | 4.06 | 76.6 | 7.4 | 10.82 | 8.02 | 3.74 |
| price_paid | mt_zstd | 2 | 22.1 | 0.16 | 9.0 | 1.8 | 1.06 | 1.09 | 1.00 |
| price_paid | mt_zstd_dict | 2 | 20.9 | 0.13 | 8.3 | 1.8 | 1.00 | 1.00 | 1.00 |
| price_paid | aa_zstd | 2 | 381.4 | 6.32 | 127.0 | 19.2 | 18.27 | 15.38 | 10.93 |
| isd_lite | mt_zstd | 8,667 | 4,634.2 | 91.80 | 114.1 | 1.2 | 0.98 | 0.97 | 0.87 |
| isd_lite | mt_zstd_dict | 8,667 | 4,751.4 | 91.15 | 117.5 | 1.3 | 1.00 | 1.00 | 1.00 |
| isd_lite | aa_zstd | 8,667 | 5,016.1 | 103.36 | 196.1 | 29.4 | 1.06 | 1.67 | 21.78 |

Slice of up to 100 rows:

| Dataset | Config | Rows per query | CPU per query (us) | Instructions per query (M) | KiB read per query | Read ops per query | CPU / dict | Bytes / dict | Ops / dict |
|---|---|---|---|---|---|---|---|---|---|
| drive_stats | mt_zstd | 97 | 445.1 | 8.67 | 19.2 | 1.0 | 1.01 | 1.29 | 1.00 |
| drive_stats | mt_zstd_dict | 97 | 442.7 | 8.50 | 14.8 | 1.0 | 1.00 | 1.00 | 1.00 |
| drive_stats | aa_zstd | 97 | 2,197.5 | 44.72 | 672.1 | 114.2 | 4.96 | 45.34 | 112.67 |
| ghcn_daily | mt_zstd | 18 | 30.1 | 0.45 | 7.0 | 1.1 | 0.94 | 1.04 | 1.00 |
| ghcn_daily | mt_zstd_dict | 18 | 32.1 | 0.44 | 6.7 | 1.1 | 1.00 | 1.00 | 1.00 |
| ghcn_daily | aa_zstd | 18 | 287.2 | 5.65 | 61.3 | 10.3 | 8.94 | 9.14 | 9.41 |
| fhvhv_trips | mt_zstd | 98 | 261.6 | 4.78 | 34.1 | 1.0 | 1.00 | 1.22 | 1.00 |
| fhvhv_trips | mt_zstd_dict | 98 | 261.1 | 4.46 | 27.9 | 1.0 | 1.00 | 1.00 | 1.00 |
| fhvhv_trips | aa_zstd | 98 | 643.1 | 12.13 | 182.4 | 30.0 | 2.46 | 6.55 | 29.80 |
| github_events | mt_zstd | 19 | 50.4 | 0.72 | 15.9 | 1.1 | 1.20 | 1.34 | 1.00 |
| github_events | mt_zstd_dict | 19 | 42.0 | 0.50 | 11.8 | 1.1 | 1.00 | 1.00 | 1.00 |
| github_events | aa_zstd | 19 | 220.7 | 3.62 | 85.3 | 12.0 | 5.25 | 7.20 | 10.57 |
| reviews | mt_zstd | 52 | 100.5 | 1.71 | 36.8 | 1.1 | 1.06 | 1.21 | 1.01 |
| reviews | mt_zstd_dict | 52 | 94.8 | 1.51 | 30.3 | 1.1 | 1.00 | 1.00 | 1.00 |
| reviews | aa_zstd | 52 | 354.3 | 6.96 | 121.1 | 12.4 | 3.74 | 4.00 | 10.84 |
| trades | mt_zstd | 100 | 144.1 | 2.69 | 29.6 | 1.0 | 0.96 | 1.04 | 1.00 |
| trades | mt_zstd_dict | 100 | 149.8 | 2.72 | 28.6 | 1.0 | 1.00 | 1.00 | 1.00 |
| trades | aa_zstd | 100 | 453.7 | 10.18 | 69.0 | 10.0 | 3.03 | 2.42 | 9.95 |
| pageviews | mt_zstd | 1 | 21.5 | 0.14 | 9.9 | 2.0 | 1.06 | 1.03 | 1.00 |
| pageviews | mt_zstd_dict | 1 | 20.4 | 0.12 | 9.6 | 2.0 | 1.00 | 1.00 | 1.00 |
| pageviews | aa_zstd | 1 | 227.0 | 4.06 | 76.8 | 7.4 | 11.15 | 7.98 | 3.74 |
| price_paid | mt_zstd | 1 | 21.6 | 0.15 | 9.1 | 1.8 | 1.03 | 1.08 | 1.00 |
| price_paid | mt_zstd_dict | 1 | 21.0 | 0.12 | 8.4 | 1.8 | 1.00 | 1.00 | 1.00 |
| price_paid | aa_zstd | 1 | 382.2 | 6.32 | 127.0 | 19.1 | 18.17 | 15.10 | 10.76 |
| isd_lite | mt_zstd | 100 | 158.0 | 3.08 | 24.6 | 1.0 | 0.93 | 0.97 | 1.00 |
| isd_lite | mt_zstd_dict | 100 | 170.0 | 3.01 | 25.3 | 1.0 | 1.00 | 1.00 | 1.00 |
| isd_lite | aa_zstd | 100 | 499.6 | 10.17 | 67.3 | 12.4 | 2.94 | 2.66 | 12.28 |

Single row:

| Dataset | Config | Rows per query | CPU per query (us) | Instructions per query (M) | KiB read per query | Read ops per query | CPU / dict | Bytes / dict | Ops / dict |
|---|---|---|---|---|---|---|---|---|---|
| drive_stats | mt_zstd | 1 | 42.1 | 0.63 | 13.4 | 1.0 | 1.00 | 1.26 | 1.00 |
| drive_stats | mt_zstd_dict | 1 | 42.2 | 0.55 | 10.7 | 1.0 | 1.00 | 1.00 | 1.00 |
| drive_stats | aa_zstd | 1 | 1,744.0 | 36.25 | 644.2 | 108.9 | 41.34 | 60.36 | 107.42 |
| ghcn_daily | mt_zstd | 1 | 26.2 | 0.38 | 6.9 | 1.1 | 0.94 | 1.02 | 1.00 |
| ghcn_daily | mt_zstd_dict | 1 | 27.8 | 0.37 | 6.8 | 1.1 | 1.00 | 1.00 | 1.00 |
| ghcn_daily | aa_zstd | 1 | 281.6 | 5.58 | 61.2 | 10.3 | 10.12 | 8.98 | 9.40 |
| fhvhv_trips | mt_zstd | 1 | 70.0 | 1.17 | 28.4 | 1.0 | 1.00 | 1.22 | 1.00 |
| fhvhv_trips | mt_zstd_dict | 1 | 69.7 | 0.96 | 23.3 | 1.0 | 1.00 | 1.00 | 1.00 |
| fhvhv_trips | aa_zstd | 1 | 446.0 | 8.36 | 164.0 | 27.2 | 6.40 | 7.03 | 26.94 |
| github_events | mt_zstd | 1 | 27.2 | 0.31 | 12.5 | 1.1 | 1.23 | 1.34 | 1.00 |
| github_events | mt_zstd_dict | 1 | 22.1 | 0.19 | 9.3 | 1.1 | 1.00 | 1.00 | 1.00 |
| github_events | aa_zstd | 1 | 203.3 | 3.31 | 80.5 | 11.6 | 9.18 | 8.66 | 10.22 |
| reviews | mt_zstd | 1 | 54.1 | 0.85 | 31.0 | 1.1 | 1.08 | 1.21 | 1.00 |
| reviews | mt_zstd_dict | 1 | 50.1 | 0.70 | 25.7 | 1.1 | 1.00 | 1.00 | 1.00 |
| reviews | aa_zstd | 1 | 308.8 | 6.06 | 111.8 | 11.8 | 6.17 | 4.35 | 10.28 |
| trades | mt_zstd | 1 | 83.2 | 1.52 | 27.8 | 1.0 | 0.93 | 1.04 | 1.00 |
| trades | mt_zstd_dict | 1 | 89.1 | 1.54 | 26.8 | 1.0 | 1.00 | 1.00 | 1.00 |
| trades | aa_zstd | 1 | 390.5 | 8.90 | 66.6 | 9.8 | 4.38 | 2.49 | 9.73 |
| pageviews | mt_zstd | 1 | 21.2 | 0.14 | 10.0 | 2.0 | 1.04 | 1.04 | 1.00 |
| pageviews | mt_zstd_dict | 1 | 20.3 | 0.12 | 9.6 | 2.0 | 1.00 | 1.00 | 1.00 |
| pageviews | aa_zstd | 1 | 225.1 | 4.06 | 76.5 | 7.4 | 11.07 | 7.95 | 3.74 |
| price_paid | mt_zstd | 1 | 21.0 | 0.15 | 8.8 | 1.8 | 1.01 | 1.05 | 1.00 |
| price_paid | mt_zstd_dict | 1 | 20.8 | 0.12 | 8.4 | 1.8 | 1.00 | 1.00 | 1.00 |
| price_paid | aa_zstd | 1 | 381.9 | 6.31 | 126.9 | 19.1 | 18.38 | 15.14 | 10.93 |
| isd_lite | mt_zstd | 1 | 105.5 | 2.05 | 23.7 | 1.0 | 0.91 | 0.98 | 1.00 |
| isd_lite | mt_zstd_dict | 1 | 115.8 | 1.99 | 24.3 | 1.0 | 1.00 | 1.00 | 1.00 |
| isd_lite | aa_zstd | 1 | 437.1 | 9.04 | 66.4 | 12.2 | 3.78 | 2.73 | 12.06 |

### 5.6 Run-to-run variation

The largest spread of the CPU time, (maximum - minimum) / median, over the
three runs (open: 12 runs), for each benchmark:

| Benchmark | Largest CPU spread | Where |
|---|---|---|
| write | 1.1% | reviews mt_zstd_dict |
| open | 62.4% | trades mt_zstd |
| full_scan | 5.2% | drive_stats mt_zstd_dict |
| partition | 5.1% | price_paid mt_zstd_dict |
| slice | 4.7% | price_paid mt_zstd_dict |
| row | 4.2% | price_paid mt_zstd_dict |

Only the open benchmark varies by more than 10%. It takes under 1.5 ms,
so scheduling noise of a few hundred microseconds shows. The other
benchmarks vary by at most 5.2%.

## 6. Analysis

### 6.1 Layout of the Parquet files

From `parquet_layout.py`. "Mean page" is the stored bytes of all leaves,
page headers and dictionary pages included, divided by the number of data
pages. "Rows per page" is rows times leaves
divided by data pages.

| Dataset | Leaves | Metadata leaves | Row groups | Rows per row group | Data pages | Mean page (B) | Rows per page | Metadata leaves share | Footer (B) |
|---|---|---|---|---|---|---|---|---|---|
| drive_stats | 171 | 2 | 2 | 178,636 | 13,699 | 817 | 4,460 | 3.3% | 27,690 |
| ghcn_daily | 10 | 2 | 2 | 1,947,219 | 6,078 | 2,279 | 6,407 | 43.5% | 1,465 |
| fhvhv_trips | 28 | 2 | 3 | 210,914 | 10,279 | 2,021 | 1,724 | 6.5% | 5,985 |
| github_events | 12 | 2 | 5 | 21,713 | 3,855 | 9,649 | 338 | 0.6% | 3,789 |
| reviews | 12 | 2 | 12 | 84,936 | 10,242 | 9,625 | 1,194 | 2.8% | 8,782 |
| trades | 11 | 2 | 3 | 619,174 | 5,544 | 3,513 | 3,686 | 0.4% | 2,143 |
| pageviews | 6 | 2 | 7 | 541,168 | 6,714 | 8,290 | 3,385 | 20.2% | 2,770 |
| price_paid | 18 | 2 | 5 | 242,448 | 12,118 | 3,334 | 1,801 | 8.4% | 5,725 |
| isd_lite | 12 | 2 | 2 | 1,238,862 | 6,759 | 1,525 | 4,399 | 0.9% | 1,871 |

### 6.2 Size

Three properties of the format explain why Parquet is smaller. Their
relative weight was not measured.

- Each leaf holds values of one column, so similar values sit together. The
  writer picks a delta, dictionary or plain encoding per page before
  compression (`sstable-parquet-format.md`, section 3.6).
- ZSTD compresses a Parquet page of up to tens of kilobytes with a 128 KiB
  window. `mt` compresses each 4 KiB chunk alone. A dictionary gives each
  chunk shared context, but not the context of its neighbours.
- `mt` stores per-row framing, such as flags, lengths and timestamp deltas.
  Parquet stores the row structure in definition levels and the two metadata
  leaves.

The dictionary narrows the gap where 4 KiB chunks share vocabulary: JSON
payloads (github_events), repeated SMART fields (drive_stats) and addresses
(price_paid). It helps little on ghcn_daily and trades, whose bytes are
mostly numbers. On isd_lite, which is also numeric, it hurts: the Data file
of `mt_zstd_dict` is 3% larger than that of `mt_zstd`. The cause was not
investigated. Parquet keeps the largest lead on exactly these three
datasets.

The timestamps are a large part of some files. The tool gives every row
the timestamp base plus its line number in `data.jsonl`. When the file order
differs from the clustering order, consecutive rows in the sstable have
unrelated timestamps. In ghcn_daily, the source is ordered by date and the
partitions by station, so `_scylla.ts` takes 43% of the Parquet file:

| Leaf | Stored (KiB) | Share of file | Data pages | Mean page (B) | Chunks with a dictionary |
|---|---|---|---|---|---|
| _scylla.ts | 5,896 | 43.3% | 806 | 7,490 | 0 |
| value | 4,754 | 34.9% | 806 | 6,040 | 0 |
| element | 800 | 5.9% | 806 | 1,017 | 2 |
| obs_date | 753 | 5.5% | 806 | 956 | 0 |
| obs_time | 526 | 3.9% | 806 | 668 | 2 |
| station_id | 363 | 2.7% | 116 | 3,204 | 0 |
| mflag | 278 | 2.0% | 208 | 1,370 | 2 |
| sflag | 112 | 0.8% | 806 | 143 | 2 |
| _scylla.kind | 29 | 0.2% | 806 | 37 | 2 |
| qflag | 17 | 0.1% | 112 | 155 | 2 |

`mt` stores the same timestamps, as row liveness timestamps, so both formats
pay for them. In trades and isd_lite, the file order is the clustering
order, and `_scylla.ts` takes 0.3% and 0.6% of the file.

### 6.3 Point queries

A Parquet reader finds the row ordinal in the trie index. It then reads, for
every leaf, the page that holds the row. A cursor decompresses and decodes
the whole page that holds the row (`sstables/parquet/reader.hh`). So a
single-row query costs about one page read and one page decode per leaf.
`mt` reads and decompresses one block of the Data file: one 4 KiB chunk, or
the compressed form of one 64 KiB row index block for a large partition.

The evidence in the tables:

- **Read requests follow the leaf count.** With the index pages cached
  after the first queries, `mt_zstd_dict` makes 1 to 2 read requests per
  single-row query. `aa_zstd` makes about one per leaf: 7.4 for pageviews
  (6 leaves), 9.8 for trades (11), 10.3 for ghcn_daily (10), 12.2 for
  isd_lite (12), 19.1 for price_paid (18) and 27.2 for fhvhv_trips (28). The pageviews and
  price_paid counts include about one index read, as for `mt`, because
  their `Partitions.db` is larger than what the queries cache. The fetcher
  reads two ranges with one request when their extents, aligned to the
  disk's read alignment (4 KiB here), overlap or touch (`fetcher.hh`). So
  small chunks of adjacent leaves share requests, and drive_stats needs 109
  requests for 171 leaves.
- **Bytes follow the page sizes.** A page holds at least 8 KiB of raw
  values, so its stored size depends on the column. A single-row query reads
  about one stored page per leaf, each rounded up to whole 4 KiB blocks,
  plus index pages. That is 61 KiB for ghcn_daily, 66 KiB for isd_lite and
  644 KiB for drive_stats.
- **CPU follows the leaves and the rows per page.** Decoding a page decodes
  hundreds to thousands of values to return one. drive_stats, with 171
  leaves, costs 41 times the CPU of `mt`. price_paid, with 18 leaves of
  mostly text and 1,800 rows per page, costs 18 times. isd_lite has the
  lowest ratio, 3.8, because its `mt` row read is expensive (section 6.9).
- **Large partitions amortise the cost.** A whole-partition read decodes
  each page once for many rows. For trades (68,131 rows per query on
  average), isd_lite (8,667 rows) and fhvhv_trips (2,479 rows), Parquet costs
  0.99, 1.06 and 1.04 of the CPU of `mt`. For drive_stats (1,125 rows of 171 leaves), it costs 1.46. For the
  other datasets, whose partitions average 2 to 99 rows, it costs 6 to 18
  times.
- **Slices fall in between.** A slice of up to 100 rows costs 2.5 to 18 times
  the CPU of `mt`. It is cheapest where the slice is 100 rows (trades,
  isd_lite, fhvhv_trips), and as expensive as a row read where the slice is one row
  (pageviews, price_paid).

For whole partitions of trades, Parquet reads about the same bytes as
`mt`, with more requests: 53 requests against 10 for `mt`. A single-partition read extends its read-ahead window one page step
at a time, doubling each time (`fetcher.hh`). The extra requests come from
that growth, and from one request per leaf per step. This is consistent with
the counts, but not verified request by request. In isd_lite, a partition
read issues 29 requests against 1.3, and reads 1.67 times the bytes of
`mt`. A partition there is about 35 KiB of the Parquet file, spread over 12
leaves. A hypothesis consistent with this: rounding each request to whole
4 KiB blocks, and reading the whole first and last page of each leaf, adds
more bytes than the partition holds.

### 6.4 Full scans

A full scan costs about the same CPU in both formats. The decode work per
value is similar, and both decompress every byte once. Parquet is cheaper on
github_events and reviews, where large text values dominate and Parquet
decompresses fewer bytes. Parquet executes 4% to 22% more user-mode
instructions per row on the other datasets, and costs up to 11% more CPU
(pageviews).

A scan of a Parquet file reads 5% to 18% more bytes than the Data file
holds. An `mt` scan reads exactly the Data file. The excess is about 2.7 KiB
per read request on every dataset, and 3.4 KiB on drive_stats. A
hypothesis consistent with this: each request covers whole 4 KiB blocks,
and the requests of adjacent column ranges each read the block that they
share. drive_stats has the most leaves, so the most range boundaries.

### 6.5 Writes

Write CPU is about equal. Net of the source scan, the ratio ranges from 0.66
to 1.21. Parquet is cheaper on github_events (0.66) and reviews (0.80).
There, compression of text is a large part of the cost. A hypothesis: Parquet
compresses fewer, larger pages, which costs less per byte. The dictionary
also speeds up `mt` there: `mt_zstd` costs 1.75 times
`mt_zstd_dict` net on github_events. Parquet is dearer on fhvhv_trips (1.21)
and price_paid (1.17). These have 28 and 18 leaves, and the writer
evaluates several encodings for every page of every leaf (section 3.6 of
`sstable-parquet-format.md`). That the encoding choice causes the extra cost
is a hypothesis. No profile was taken.

### 6.6 Opening an sstable

Opening a Parquet sstable reads and parses the footer. Opening
an `mt` sstable reads the whole `CompressionInfo.db`, which holds the chunk
offsets and, for `mt_zstd_dict`, the dictionary. In these runs, opening a
Parquet sstable costs less CPU and fewer bytes than opening `mt_zstd_dict`.
These costs are below 1.5 ms, and a node opens an sstable once.

### 6.7 The trades run

The first run took under 3 minutes for each dataset except
trades, which took about 11 minutes. The slow phase is the whole-partition
benchmark, in all three configurations equally.

- trades has 28 partitions of 36,551 to 302,218 rows. The benchmark reads
  1000 partitions chosen uniformly with replacement. So one run reads
  68,131,000 rows, 36.7 times the dataset.
- One run of that benchmark takes about 42 seconds in each configuration.
  Nine measured runs and three unmeasured validation passes take about 8.4
  of the 10.8 minutes of the rerun.
- The CPU per row is the same as in a full scan. For `mt_zstd_dict`, it is
  608 ns in a partition read and 620 ns in a full scan. For `aa_zstd`, it is
  602 ns and 595 ns.
- Parquet issues 5.3 times the read requests of `mt` in this benchmark, for
  0.96 times the bytes (section 6.3).

So the run time is a consequence of the benchmark's design on a dataset of
few, large partitions. It is not a pathology of the Parquet reader or
writer, and it does not bias the comparison: every configuration reads the
same partitions.

### 6.8 A correction to the tool

The first run also showed that opening a Parquet sstable read one page of
every leaf, twice: 207 requests and 1.5 MiB for drive_stats, against 17
requests and 204 KiB now. The cause was the
tool. `test_env::reusable_sst()` loads the first and last clustering
positions by default. That reads the first and the last partition, and a
node does not do it (`sstable_open_config`, default false). For Parquet,
this costs one page per leaf for each of the two partitions. The tool now opens sstables as a node
does. The eight datasets of the first run were rerun with the corrected
tool. This document shows the rerun. isd_lite was added later and ran only
with the corrected tool. The sstables of the rerun are identical in size to
those of the first run. Apart from the open benchmark, the median CPU times of the two runs
differ by at most 4.2%.

### 6.9 Two weather time series: isd_lite and ghcn_daily

Both datasets are NOAA station observations with int values, and both
have one partition per station. They differ in shape:

| Property | ghcn_daily | isd_lite |
|---|---|---|
| Values per row | 1 (`element` is a clustering column) | Up to 8, one column each |
| Partitions | 38,070 | 286 |
| Rows per partition, median | 71 | 8,699 |
| Leaves | 10 | 12 |
| File order | Date first, so not the clustering order | The clustering order |
| `_scylla.ts` share of the Parquet file | 43.3% | 0.6% |

The leaves of isd_lite:

| Leaf | Stored (KiB) | Share of file | Data pages | Mean page (B) | Chunks with a dictionary |
|---|---|---|---|---|---|
| wind_direction | 2,269 | 22.3% | 730 | 3,183 | 2 |
| air_temp | 2,039 | 20.1% | 773 | 2,702 | 1 |
| wind_speed | 1,965 | 19.4% | 767 | 2,624 | 2 |
| dew_point | 1,575 | 15.5% | 656 | 2,458 | 1 |
| sea_level_pressure | 1,097 | 10.8% | 502 | 2,237 | 0 |
| sky_condition | 424 | 4.2% | 440 | 987 | 2 |
| precip_1h | 306 | 3.0% | 416 | 754 | 2 |
| obs_time | 153 | 1.5% | 778 | 201 | 0 |
| precip_6h | 145 | 1.4% | 70 | 2,116 | 1 |
| _scylla.ts | 59 | 0.6% | 778 | 78 | 0 |
| _scylla.kind | 28 | 0.3% | 778 | 37 | 2 |
| station_id | 8 | 0.1% | 71 | 120 | 0 |

The effects:

- **Size.** Parquet takes 0.32 of `mt_zstd_dict` on isd_lite and 0.48 on
  ghcn_daily. Per cell, it stores 0.80 bytes on isd_lite and 1.44 on
  ghcn_daily. `mt_zstd_dict` stores 2.51 and 3.02. On isd_lite, each value
  column holds the hourly series of one station in time order, and the
  timestamp leaves cost little. In ghcn_daily, unrelated row timestamps take
  43% of the file.
- **Writes and full scans.** Both cost about the same as `mt` on both
  datasets: 0.98 and 1.00 on isd_lite, 1.07 and 1.05 on ghcn_daily.
- **Whole partitions.** On isd_lite, Parquet costs 1.06 of the CPU of `mt`.
  On ghcn_daily, with 99 rows per query on average, it costs 5.95 times.
- **Single rows.** Parquet costs 437 us per row on isd_lite and 282 us on
  ghcn_daily, about one page per leaf in both. `mt` costs 116 us and 28 us.
  An isd_lite partition is about 111 KiB of the `mt` Data file, so `mt`
  reads the compressed form of one 64 KiB row index block (24 KiB per query
  against 7 KiB on ghcn_daily) and decodes up to about 1,500 rows. So the
  ratio is 3.8 on isd_lite and 10.1 on ghcn_daily. The ratio is lower
  because `mt` costs more, not because Parquet costs less.

## 7. Limitations

- **Dev-mode build.** Dev mode compiles with `-O2`, without the advanced
  optimizations of release mode, links Seastar as shared libraries, and
  enables error injection, allocation failure injection and the preemption
  source. So absolute CPU times are not representative of a release build.
  Only ratios between configurations are meaningful. Even the ratios may
  shift in release mode, because the two formats spend their time in
  different code.
- **One sstable per configuration.** Each configuration is one sstable
  written from one memtable. The benchmark does not measure compaction,
  memtable flush beyond the sstable writer, reads that merge several
  sstables, or the effect of sstable count.
- **No concurrency.** One reader runs at a time on one shard. Parquet issues
  more, smaller reads per point query. On a loaded disk, their latency and
  their share of the device's IOPS could matter more than here.
- **No row cache.** Every point query reads the Data file. A node with a
  warm row cache reads sstables less often.
- **Storage.** The files are on btrfs with transparent ZSTD compression,
  over LUKS encryption, on an NVMe SSD. Seastar's direct I/O may be served
  differently on such a file system, and the page cache was not dropped.
  This affects wall time, which the analysis does not use. It may also
  affect the system part of CPU time. User-mode instruction counts should not be affected.
- **Small sstables.** Each input is about 400 MB of JSON lines. The
  `mt_zstd_dict` sstables are 15 to 110 MiB and the `aa_zstd` sstables 10
  to 95 MiB (section 5.1). The Parquet files have 2 to 12 row groups. Larger
  sstables have more row groups, but the same row group and page sizes, so
  the per-query costs should change little. This is not verified.
- **Dictionary trained on the same data.** The dictionary is trained on
  samples of the data it then compresses. This is what production does for
  the sstables written after a dictionary is trained, for example by a major
  compaction. Data written later, with a dictionary trained on older data,
  may compress less well.
- **One timestamp per row.** All cells of a row share one timestamp, and
  there are no TTLs or deletions. So the Parquet files have only two
  metadata leaves. Data with per-cell timestamps, TTLs or tombstones would
  add metadata leaves to Parquet and metadata to `mt`.
- **Three runs.** Medians of three runs hide little noise here (section 5.6),
  but they are not a statistical test.
- **Licences.** The datasets are not part of the repository. Backblaze
  Drive Stats requires attribution to Backblaze and may not be sold.
  Amazon Reviews 2023 is published for research and has no explicit
  licence. GH Archive has no explicit licence, and the event contents are
  subject to GitHub's terms. Binance market data is published under its
  terms of use, without an open licence. ISD-Lite is limited to US
  stations, because the non-US data in ISD may not be redistributed (WMO
  Resolution 40). The other sources are public domain or under open
  licences (section 4).

## 8. How to reproduce

Run these commands from the repository root, inside the dbuild container
(`tools/toolchain/dbuild`). `DATA` and `RESULTS` are directories of your
choice outside the repository. The downloads take about 3 GB, the prepared
datasets about 3.5 GB and the results about 1.5 GB.

1. Download and convert the datasets. This needs network access and, for
   `fhvhv_trips`, the Python module `pyarrow`.

        test/perf/sstable_datasets/prepare_all.sh $DATA

   To prepare one dataset, name it:

        test/perf/sstable_datasets/prepare_all.sh $DATA isd_lite

   For isd_lite, `download.sh` fetches the station list, asks
   `conv_isd_lite.py --candidates` for the 600 candidate stations, and
   fetches their 2023 files four at a time.

   It writes `$DATA/prepared/<name>/{schema.cql,data.jsonl,meta.json}` and
   validates each dataset. Each converter selects its subset by a fixed
   rule, so the subsets are the same as long as the source files do not
   change. The sources may change or disappear.

2. Build Scylla in dev mode.

        ninja build/dev/scylla

3. Run every dataset, one after the other.

        test/perf/sstable_datasets/run_all.sh $DATA $RESULTS

   To run one dataset:

        test/perf/sstable_datasets/run_dataset.sh $DATA/prepared/trades $RESULTS/trades

   Extra arguments go to the tool, for example `--runs 5 --keys 2000`.
   An output directory must not hold the sstables of an earlier run.

4. Make the tables of this document.

        python3 test/perf/sstable_datasets/results_tables.py $RESULTS --datasets $DATA
        python3 test/perf/sstable_datasets/parquet_layout.py $RESULTS
        python3 test/perf/sstable_datasets/parquet_layout.py $RESULTS --leaves ghcn_daily
        python3 test/perf/sstable_datasets/parquet_layout.py $RESULTS --leaves isd_lite

   `results_tables.py --table summary` prints one table. Both scripts need
   only the Python standard library.
