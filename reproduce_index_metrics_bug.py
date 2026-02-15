#!/usr/bin/env python3
"""
Script to reproduce the double metrics registration bug when a table with
a secondary index is dropped and recreated while queries are running.

Prerequisites:
    pip install cassandra-driver asyncio

The bug scenario:
1. Create a table with a secondary index
2. Start concurrent SELECT queries using the index (in threads)
3. While queries are in-flight, DROP and CREATE the table with same index name
4. If queries hold stats references when the new table's index is created,
   metrics registration will conflict

Run with: python3 reproduce_index_metrics_bug.py
"""

import sys
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement, ConsistencyLevel
import time
import traceback
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed


class IndexMetricsBugReproducer:
    def __init__(self, host='127.0.0.1', port=9042):
        self.host = host
        self.port = port
        self.cluster = None
        self.session = None
        self.keyspace = 'test_index_metrics_ks'
        self.table = 'test_table'
        self.index_name = 'test_idx'

    def connect(self):
        """Connect to ScyllaDB"""
        print(f"Connecting to ScyllaDB at {self.host}:{self.port}...")
        self.cluster = Cluster([self.host], port=self.port)
        self.session = self.cluster.connect()
        print("Connected successfully")

    def setup_schema(self):
        """Create keyspace and table with secondary index"""
        print(f"\nSetting up schema...")

        # Drop keyspace if exists
        self.session.execute(f"DROP KEYSPACE IF EXISTS {self.keyspace}")

        # Create keyspace
        self.session.execute(f"""
            CREATE KEYSPACE {self.keyspace}
            WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
        """)

        self.session.set_keyspace(self.keyspace)

        # Create table
        self.session.execute(f"""
            CREATE TABLE {self.table} (
                pk int,
                ck int,
                value text,
                indexed_col int,
                PRIMARY KEY (pk, ck)
            )
        """)

        # Create secondary index
        self.session.execute(f"""
            CREATE INDEX {self.index_name} ON {self.table} (indexed_col)
        """)

        print(f"Created keyspace {self.keyspace}, table {self.table}, and index {self.index_name}")

    def populate_data(self, num_rows=1000):
        """Insert test data"""
        print(f"\nPopulating {num_rows} rows...")

        for i in range(num_rows):
            self.session.execute(f"""
                INSERT INTO {self.table} (pk, ck, value, indexed_col)
                VALUES ({i}, {i}, 'value_{i}', {i % 100})
            """)

        print(f"Inserted {num_rows} rows")

    def run_index_query(self, query_id, indexed_value, duration_seconds=5):
        """Run a query using the secondary index for a specified duration"""
        t_start_query = time.time()
        print(f"[Query {query_id} T={t_start_query:.3f}] Starting index query loop for {duration_seconds}s...")

        query = f"""
            SELECT pk, ck, value, indexed_col
            FROM {self.keyspace}.{self.table}
            WHERE indexed_col = {indexed_value}
        """

        start_time = time.time()
        count = 0
        success_count = 0
        error_count = 0
        first_success_time = None

        # Keep trying for the full duration, even if queries fail temporarily
        while time.time() - start_time < duration_seconds:
            try:
                t_before = time.time()
                # Execute the query (this uses the secondary index)
                result = self.session.execute(query)
                rows = list(result)
                t_after = time.time()
                count += 1
                success_count += 1

                if first_success_time is None:
                    first_success_time = t_after
                    print(f"[Query {query_id} T={t_after:.3f}] First successful query (took {t_after-t_before:.3f}s)")

                # Small delay to allow interleaving
                time.sleep(0.01)

            except Exception as e:
                error_count += 1
                # Don't stop on error - table might be dropped temporarily
                # Just wait a bit and try again
                if error_count == 1:
                    t_err = time.time()
                    print(f"[Query {query_id} T={t_err:.3f}] Got error (table may be dropped): {type(e).__name__}")
                time.sleep(0.1)

        t_end = time.time()
        print(f"[Query {query_id} T={t_end:.3f}] Completed: {success_count} successful, {error_count} errors (duration {t_end-t_start_query:.3f}s)")
        return True

    def drop_and_recreate_table(self, delay_before_drop=1.0):
        """Drop and recreate the table with the same index"""
        print(f"\n[Schema Change] *** SCHEMA CHANGE TASK STARTED ***")
        print(f"[Schema Change] Waiting {delay_before_drop}s before dropping table...")
        time.sleep(delay_before_drop)

        try:
            t_start = time.time()
            print(f"[Schema Change T={t_start:.3f}] *** BEGINNING DROP TABLE {self.table} ***")
            self.session.execute(f"DROP TABLE {self.keyspace}.{self.table}")
            t_drop = time.time()
            print(f"[Schema Change T={t_drop:.3f}] *** TABLE DROPPED (took {t_drop-t_start:.3f}s) ***")

            # Small delay between drop and create
            time.sleep(0.1)

            t_start_create = time.time()
            print(f"[Schema Change T={t_start_create:.3f}] Recreating table {self.table}...")
            self.session.execute(f"""
                CREATE TABLE {self.keyspace}.{self.table} (
                    pk int,
                    ck int,
                    value text,
                    indexed_col int,
                    PRIMARY KEY (pk, ck)
                )
            """)
            t_table = time.time()
            print(f"[Schema Change T={t_table:.3f}] Table created (took {t_table-t_start_create:.3f}s)")

            print(f"[Schema Change T={t_table:.3f}] Creating index {self.index_name}...")
            t_idx_start = time.time()
            self.session.execute(f"""
                CREATE INDEX {self.index_name} ON {self.keyspace}.{self.table} (indexed_col)
            """)
            t_idx = time.time()
            print(f"[Schema Change T={t_idx:.3f}] Index created (took {t_idx-t_idx_start:.3f}s)")

            print(f"[Schema Change T={t_idx:.3f}] Table and index recreated successfully")

            # Re-populate some data
            print(f"[Schema Change T={t_idx:.3f}] Re-populating data...")
            for i in range(100):
                self.session.execute(f"""
                    INSERT INTO {self.keyspace}.{self.table} (pk, ck, value, indexed_col)
                    VALUES ({i}, {i}, 'value_{i}', {i % 100})
                """)
            t_end = time.time()
            print(f"[Schema Change T={t_end:.3f}] Schema change completed (total {t_end-t_start:.3f}s)")

        except Exception as e:
            t_err = time.time()
            print(f"[Schema Change T={t_err:.3f}] *** EXCEPTION OCCURRED ***")
            print(f"[Schema Change] ERROR: {e}")
            print(f"[Schema Change] Exception type: {type(e).__name__}")
            traceback.print_exc()
            raise  # Re-raise to make failure visible

    def reproduce_bug(self, num_concurrent_queries=10, query_duration=5):
        """
        Main reproduction logic:
        1. Start multiple concurrent queries using the index (in threads)
        2. While queries are running, DROP and CREATE the table with same index name
        3. If old stats references persist, metrics registration will conflict

        Expected timeline:
        - T=0s: Start queries in threads
        - T=0.01s: First queries succeed and acquire stats references (held for 5s)
        - T=1s: DROP TABLE begins
        - T=1-5s: Schema operations with delays
        - T=4s: New table/index created, tries to register metrics
        - CRITICAL: If old stats still alive → double registration!
        """
        t_start_test = time.time()
        print(f"\nT={t_start_test:.3f} {'='*70}")
        print("REPRODUCING DOUBLE METRICS REGISTRATION BUG")
        print(f"{'='*70}")
        print(f"Starting {num_concurrent_queries} query threads and 1 schema change thread...")

        # Use ThreadPoolExecutor for true parallelism
        with ThreadPoolExecutor(max_workers=num_concurrent_queries + 1) as executor:
            # Submit query tasks
            query_futures = []
            for i in range(num_concurrent_queries):
                indexed_value = i % 10  # Query different indexed values
                future = executor.submit(self.run_index_query, i, indexed_value, query_duration)
                query_futures.append(future)

            print(f"Submitted {len(query_futures)} query tasks")

            # Submit schema change task
            print(f"Submitting schema change task...")
            schema_future = executor.submit(self.drop_and_recreate_table, 1.0)
            print(f"Schema change task submitted")

            # Wait for all tasks to complete
            print(f"\nWaiting for all threads to complete...")
            all_futures = query_futures + [schema_future]

            successful_queries = 0
            failed_queries = 0
            schema_success = False
            exceptions = []

            for i, future in enumerate(as_completed(all_futures)):
                try:
                    result = future.result()
                    if future == schema_future:
                        schema_success = True
                        print(f"Schema change thread completed successfully")
                    else:
                        if result:
                            successful_queries += 1
                        else:
                            failed_queries += 1
                except Exception as e:
                    if future == schema_future:
                        print(f"Schema change thread failed with exception: {e}")
                        exceptions.append(("schema_change", e))
                    else:
                        failed_queries += 1
                        exceptions.append((f"query_{i}", e))

        # Report results
        print(f"\n{'='*70}")
        print("RESULTS")
        print(f"{'='*70}")
        print(f"Successful query threads: {successful_queries}/{num_concurrent_queries}")
        print(f"Failed query threads: {failed_queries}/{num_concurrent_queries}")
        print(f"Schema change thread: {'Success' if schema_success else 'Failed'}")

        if exceptions:
            print("\n⚠️  EXCEPTIONS OCCURRED:")
            for name, exc in exceptions:
                print(f"  {name}: {exc}")

        print(f"\n{'='*70}")
        print("If the bug occurs, check ScyllaDB logs for:")
        print("  - Double registration exception")
        print("  - Metrics registration errors")
        print(f"{'='*70}\n")

    def cleanup(self):
        """Cleanup resources"""
        if self.cluster:
            self.cluster.shutdown()
            print("\nDisconnected from ScyllaDB")

    def run(self):
        """Main execution flow"""
        try:
            # Setup
            print("\n=== SETUP PHASE ===")
            self.connect()
            print("Connected to ScyllaDB")

            self.setup_schema()
            print("Schema created")

            self.populate_data()
            print(f"Data populated")

            print("\n=== STARTING BUG REPRODUCTION ===")
            # Reproduce the bug
            self.reproduce_bug(
                num_concurrent_queries=20,  # More queries = higher chance of bug
                query_duration=10  # Queries run for 10 seconds (long enough to overlap with schema changes)
            )
            print("\n=== BUG REPRODUCTION COMPLETED ===")

        finally:
            self.cleanup()


def main():
    print("""
╔════════════════════════════════════════════════════════════════════╗
║  Index Metrics Double Registration Bug Reproducer                 ║
║                                                                    ║
║  This script drops and recreates a table with a secondary index   ║
║  while queries are in-flight (using threads for parallelism),     ║
║  potentially causing double metrics registration if old stats     ║
║  references persist.                                              ║
╚════════════════════════════════════════════════════════════════════╝
    """)

    reproducer = IndexMetricsBugReproducer()
    reproducer.run()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\nFatal error: {e}")
        traceback.print_exc()
        sys.exit(1)
