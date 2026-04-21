# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

# Tests for stream operations' nuanses that are intristic to ScyllaDB (parent-children relationship on stream shards).

import time, random, collections

import pytest
from test.alternator.test_streams import create_stream_test_table, wait_for_active_stream

TABLET_TAGS = [{'Key': 'system:initial_tablets', 'Value': '0'}]

# get table_id from keyspace and table name
def get_table_or_view_id(cql, keyspace: str, table: str):
    rows = cql.execute(f"select id from system_schema.tables where keyspace_name = '{keyspace}' and table_name = '{table}'")
    try:
        row = rows.one()
    except Exception:
        row = None
    if row is not None:
        return row.id
    rows = cql.execute(f"select id from system_schema.views where keyspace_name = '{keyspace}' and view_name = '{table}'")
    return rows.one().id

# get user table id from cdc_log table id
def get_base_table(cql, table_id):
    # copied from tablets.py:get_base_table
    # this might return more than one row, but we don't care as all rows
    # will have the same base_table value
    rows = cql.execute(f"SELECT base_table FROM system.tablets where table_id = {table_id} limit 1")
    return rows.one().base_table

# validate that streams tables are synchronized with tablets count - all new cdc shards have been created
def assert_number_of_streams_is_equal_to_number_of_tablets(rest_api, cql, ks, table_name, cdc_log_table_name):
    # Retry with exponential backoff, as on slow builds (e.g. debug aarch64)
    # the background CDC stream regeneration may take a while to catch up.
    sleep = 0.1
    for x in range(0, 7):
        # Don't sleep on the first try - we might be already in sync.
        if x > 0:
            time.sleep(sleep)
            sleep *= 2
        tablet_count = get_tablet_count_for_base_table_of_table(rest_api, cql, ks, cdc_log_table_name)
        ts = cql.execute(f"SELECT toUnixTimestamp(timestamp) AS ts FROM system.cdc_timestamps WHERE keyspace_name='{ks}' AND table_name='{table_name}' ORDER BY timestamp DESC LIMIT 1").one().ts
        CdcStreamState_CURRENT = 0 # from test.cluster.test_cdc_with_tablets.CdcStreamState.CURRENT
        count = cql.execute(f"SELECT count(*) FROM system.cdc_streams WHERE keyspace_name='{ks}' AND table_name='{table_name}' AND timestamp = {ts} AND stream_state = {CdcStreamState_CURRENT} limit 1").one().count
        if count == tablet_count:
            break
    assert count == tablet_count

# return tablet count for given cdc_log table (table that holds cdc data for given user table)
def get_tablet_count_for_base_table_of_table(rest_api, cql, keyspace_name: str, table_name: str):
    table_id = get_table_or_view_id(cql, keyspace_name, table_name)
    table_id = get_base_table(cql, table_id)
    # this might return more than one row, but we don't care as all rows
    # will have the same tablet_count value
    rows = cql.execute(f"SELECT tablet_count FROM system.tablets where table_id = {table_id} limit 1")
    return rows.one().tablet_count

# modify tablet count for given cdc_log table and wait until the change is applied
# this calls alter table with new `min_tablet_count`, which requires additional scylla options to work reliably
# (--tablet-load-stats-refresh-interval-in-seconds=1 and --tablets-initial-scale-factor=1)
def set_tablet_count_and_wait(rest_api, cql, ks, table_name, cdc_log_table_name, expected_tablet_count):
    assert_number_of_streams_is_equal_to_number_of_tablets(rest_api, cql, ks, table_name, cdc_log_table_name)
    cql.execute(f"ALTER TABLE \"{ks}\".\"{cdc_log_table_name}\" WITH tablets = {{'min_tablet_count': {expected_tablet_count}}};")
    start = time.time()
    while time.time() < start + 10:
        if get_tablet_count_for_base_table_of_table(rest_api, cql, ks, cdc_log_table_name) == expected_tablet_count:
            break
        time.sleep(0.1)
    else:
        pytest.fail(f'Tablet count did not reach expected value {expected_tablet_count} within timeout')

def iterate_over_describe_stream(dynamodbstreams, arn, end_ts, filter_shard_id=None):
    params = {
        'StreamArn': arn
    }
    if filter_shard_id is not None:
        params['ShardFilter'] = {
            'Type': 'CHILD_SHARDS',
            'ShardId': filter_shard_id
        }
    desc = dynamodbstreams.describe_stream(**params)

    while True:
        assert time.time() <= end_ts, "Time ran out"
        shards = desc['StreamDescription']['Shards']

        for shard in shards:
            yield shard
            assert time.time() <= end_ts, "Time ran out"

        last_shard = desc["StreamDescription"].get("LastEvaluatedShardId")
        if not last_shard:
            break

        desc = dynamodbstreams.describe_stream(ExclusiveStartShardId=last_shard, **params)

# helper function for parent-child relationship test - it will:
#  - drive changes in tablet count
#  - wait for expected number of stream shards to be created
#  - build parent map (shard -> parent) and children map (shard -> list of children) between stream shards
#  - call the callback with the data
def run_parent_children_relationship_test(dynamodb, dynamodbstreams, rest_api, cql, tablet_multipliers, callback):
    with create_stream_test_table(dynamodb, StreamViewType='NEW_AND_OLD_IMAGES', Tags=TABLET_TAGS) as table:
        (arn, label) = wait_for_active_stream(dynamodbstreams, table)

        ks = f'alternator_{table.name}'
        table_name = table.name
        cdc_log_table_name = f'{table_name}_scylla_cdc_log'
        # Record initial tablet count, which determines the expected
        # number of root stream shards (shards without a parent).
        init_table_count = get_tablet_count_for_base_table_of_table(rest_api, cql, ks, cdc_log_table_name)

        # Drive tablet count changes according to tablet_multipliers.
        # Each change triggers a new "generation" of stream shards to be created.
        # tablet multiplier might be less than 1
        for tablet_mult in tablet_multipliers:
            tablet_count = int(init_table_count * tablet_mult)
            assert tablet_count >= 1
            set_tablet_count_and_wait(rest_api, cql, ks, table_name, cdc_log_table_name, tablet_count)

        # Strip multipliers that don't change the tablet count from the
        # previous state. The table starts with a multiplier of 1 (i.e.
        # init_table_count tablets), so a leading 1 is a no-op. Subsequent
        # duplicates are also no-ops since ALTER TABLE with the current
        # tablet count doesn't trigger a new CDC stream generation.
        effective_multipliers = []
        for m in tablet_multipliers:
            if effective_multipliers:
                if m != effective_multipliers[-1]:
                    effective_multipliers.append(m)
            elif m != 1:
                effective_multipliers.append(m)

        # The total number of stream shards across all generations:
        # 1) The root generation created when the table is first created.
        total_shard_count = init_table_count
        # 2) One generation per effective multiplier.
        total_shard_count += int(sum(effective_multipliers) * init_table_count)

        # Poll DescribeStream until all expected stream shards have appeared,
        # building a shard_id -> parent_shard_id map (shard_parents_map) and
        # collecting root shard IDs (shards with no parent).
        end_ts = time.time() + 30
        root_shard_ids = []
        shard_parents_map = {}
        while time.time() < end_ts:
            root_shard_ids = []
            shard_parents_map = {}

            for shard in iterate_over_describe_stream(dynamodbstreams, arn, end_ts):
                shard_id = shard['ShardId']
                parent_shard_id = shard.get('ParentShardId', None)
                assert shard_id not in shard_parents_map
                if parent_shard_id is None:
                    root_shard_ids.append(shard_id)
                shard_parents_map[shard_id] = parent_shard_id
            
            if len(shard_parents_map) >= total_shard_count:
                break

            time.sleep(0.1)

        # For each shard, use DescribeStream's CHILD_SHARDS filter to
        # build a shard_id -> [child_shard_ids] map (shard_children_map).

        shard_children_map = {}
        for shard_id in shard_parents_map:
            filter_children = []
            for child_shard in iterate_over_describe_stream(dynamodbstreams, arn, end_ts, filter_shard_id=shard_id):
                child_shard_id = child_shard['ShardId']
                filter_children.append(child_shard_id)
            shard_children_map[shard_id] = filter_children

        callback(root_shard_ids, shard_parents_map, shard_children_map)

# run a test, where we create two cdc log generations (parent and children)
# and children count is half of parents (thus parents are merged into children)
# validate merge assumptions:
# - every two parents has the same child
# - only half of parents is marked as parent by any child (because child can provide only single parent and in case of merge it has two parents)
# - various sizes
# NOTE: this test assumes starting tablet count is bigger than one
# NOTE: this test fails if `system:initial_tablets` is set to anything but 0 - maybe because 0 means start with some default
#       while non-zero means start with this value, but never go any lower?
@pytest.mark.xfail(reason="Tablet merges are blocked when Alternator Streams are enabled")
def test_parent_children_merge(dynamodb, dynamodbstreams, rest_api, cql):
    def verify_parent_children_relationship_merge(root_shard_ids, shard_parents_map, shard_children_map):
        # shard_parents_map will contain both generations, so we strip original one
        shard_parents_map = { shard_id: parent_id for shard_id, parent_id in shard_parents_map.items() if parent_id is not None }

        # shard_children_map will contain both generations, but only original generation will have children, so we strip non-children ones
        shard_children_map = { shard_id: children for shard_id, children in shard_children_map.items() if children }

        # shard_parents_map contains child -> parent map, thus it's size is equal to total number of children, which is half of parents
        assert len(shard_parents_map) * 2 == len(root_shard_ids)

        # shard_children_map contains parent -> list of children map, thus it's size is equal to total number of parents
        # which must be equal to root_shard_ids
        assert len(root_shard_ids) == len(shard_children_map)

        # all parents must be from first (original) generation (`root_shard_ids`)
        assert sorted(shard_children_map.keys()) == sorted(root_shard_ids)
        
        # only half of parents will show up as parents in shard_parents_map, but all of them must be from root_shard_ids
        assert len(shard_parents_map.values()) * 2 == len(root_shard_ids)
        assert set(root_shard_ids).issuperset(set(shard_parents_map.values()))

        # every parent has exactly one child - we're merging
        assert all(len(children) == 1 for children in shard_children_map.values())
        
        # every child must occur exactly twice in children lists - we're merging (2 -> 1) so
        # for every two parents there will be a one child, thus two parents have the same child as children
        existence_count = collections.defaultdict(int)
        for children in shard_children_map.values():
            for child in children:
                existence_count[child] += 1
        assert all(count == 2 for count in existence_count.values())

    run_parent_children_relationship_test(dynamodb, dynamodbstreams, rest_api, cql, [0.5], verify_parent_children_relationship_merge)

# run a test, where we create two cdc log generations (parent and children)
# and children count is double of parents (thus parents are splited into children)
# validate split assumptions:
# - every parents has the two distinct children
# - every two children has the same parent
# - various sizes
def test_parent_children_split(dynamodb, dynamodbstreams, rest_api, cql):
    def verify_parent_children_relationship_split(root_shard_ids, shard_parents_map, shard_children_map):
        # shard_parents_map will contain both generations, so we strip original one
        shard_parents_map = { shard_id: parent_id for shard_id, parent_id in shard_parents_map.items() if parent_id is not None }

        # shard_children_map will contain both generations, but only original generation will have children, so we strip non-children ones
        shard_children_map = { shard_id: children for shard_id, children in shard_children_map.items() if children }

        # shard_parents_map contains child -> parent map, thus it's size is equal to total number of children, which is double of parents
        assert len(shard_parents_map) == len(root_shard_ids) * 2

        # shard_children_map contains parent -> list of children map, thus it's size is equal to total number of parents
        # which must be equal to root_shard_ids
        assert len(root_shard_ids) == len(shard_children_map)

        # all parents must be from first (original) generation (`root_shard_ids`)
        assert sorted(shard_children_map.keys()) == sorted(root_shard_ids)
        
        # every parent must show up shard_parents_map twice
        # set of parents must equal `root_shard_ids`
        assert len(shard_parents_map.values()) == len(root_shard_ids) * 2
        assert set(root_shard_ids) == set(shard_parents_map.values())
        existence_count = collections.defaultdict(int)
        for child in shard_parents_map.values():
            existence_count[child] += 1
        assert all(count == 2 for count in existence_count.values())

        # every parent has exactly two children - we're splitting
        assert all(len(children) == 2 for children in shard_children_map.values())
        
        # every child must occur exactly once in children lists - we're splitting (2 -> 1) so
        # for every parent there will be two distinct children
        all_children = []
        for children in shard_children_map.values():
            for child in children:
                all_children.append(child)
        assert len(set(all_children)) == len(all_children)

    run_parent_children_relationship_test(dynamodb, dynamodbstreams, rest_api, cql, [2], verify_parent_children_relationship_split)

# the test will:
#   - create a table with streams enabled
#   - get initial tablet count
#   - for each multiplier in tablet_multipliers:
#       - modify tablet count to initial * multiplier - this will trigger
#         creation of new stream shards in the stream as currently we have 1 to 1
#         mapping between tablets and stream shards
#   - after all modifications, use DescribeStream to get all stream shards
#   - build a map of stream shard -> parent stream shard (shard_parents_map) and
#     a map of stream shard -> child stream shards (shard_children_map)
#   - verify that:
#       - number of root stream shards (stream shards without parent) is correct -
#         should be equal to initial tablet count
#       - starting from root stream shards and following children, each path
#         down the tree has length equal to len(tablet_multipliers) -
#         this is because every change in tablet count causes new generation
#         of stream shards to be created. Each stream shard from previous generation will point
#         to at least one stream shard in the next generation and never to some other generation.
def test_parent_filtering(dynamodb, dynamodbstreams, rest_api, cql):
    # Only growing tablet count — merges are incompatible with Alternator Streams.
    tablet_multipliers = [1, 2, 4, 8, 16]

    def verify_parent_children_relationship(root_shard_ids, shard_parents_map, shard_children_map):
        # Verify the split invariant: all children point
        # back to the same parent.
        #
        # First, build declared_children from the ParentShardId field recorded
        # in shard_parents_map, then compare against the CHILD_SHARDS filter
        # results to confirm consistency.
        declared_children = {}  # parent_shard_id -> [child_shard_ids derived from ParentShardId]
        for shard_id, parent_shard_id in shard_parents_map.items():
            if parent_shard_id is not None:
                declared_children.setdefault(parent_shard_id, []).append(shard_id)

        all_shards = set()
        in_children = set()
        for shard_id in shard_parents_map:
            all_shards.add(shard_id)
            filter_children = shard_children_map[shard_id]
            for child_shard_id in filter_children:
                in_children.add(child_shard_id)
            # Verify that every child declared via ParentShardId is also returned
            # by the CHILD_SHARDS filter for this shard.
            declared = set(declared_children.get(shard_id, []))
            assert declared.issubset(set(filter_children)), \
                f"Declared children {declared} not subset of filter children {set(filter_children)} for shard {shard_id}"
            # Between generations (currently) we can have either splits by half or merges two into one.
            # In case of split - children count will be > 1 and all children will point exactly to parent
            #   (so declared == filter_children).
            # In case of merge - the CHILD_SHARDS filter returns the merged child for both parents,
            #   but the child's ParentShardId can only point to one of them. So the non-designated
            #   parent will have filter_children = [child] but declared_children = [].
            # This assert checks that either all filter children were declared (split case) or
            # we have at most one child (merge case where this parent isn't the designated one).
            assert set(filter_children) == declared or len(filter_children) <= 1, \
                f"Unexpected children mismatch for shard {shard_id}: filter={filter_children}, declared={list(declared)}"

        # Verify split/merge invariants across generation boundaries.
        # Group shards by generation (extracted from the hex timestamp in the shard ID).
        def get_gen(t):
            return t[2:13]
        gen_shards = {}
        for shard_id in shard_parents_map:
            gen = get_gen(shard_id)
            gen_shards.setdefault(gen, []).append(shard_id)
        # Sort generations chronologically and verify parent/child ratios.
        sorted_gens = sorted(gen_shards.keys())
        for i in range(1, len(sorted_gens)):
            prev_gen = sorted_gens[i - 1]
            curr_gen = sorted_gens[i]
            parent_count = len(gen_shards[prev_gen])
            child_count = len(gen_shards[curr_gen])
            if child_count > parent_count:
                # Split: 2x children, every parent has 2 children pointing to it
                assert child_count == 2 * parent_count, \
                    f"Split ratio wrong: {parent_count} parents -> {child_count} children"
                for p in gen_shards[prev_gen]:
                    kids = declared_children.get(p, [])
                    assert len(kids) == 2, f"Split parent {p} has {len(kids)} declared children, expected 2"
            elif child_count < parent_count:
                # Merge: 2x parents, every child has 2 parents (but only 1 declared via ParentShardId)
                assert parent_count == 2 * child_count, \
                    f"Merge ratio wrong: {parent_count} parents -> {child_count} children"
                # Each child in the current generation should appear as a filter_child
                # of exactly 2 parents from the previous generation.
                for c in gen_shards[curr_gen]:
                    parents_of_c = [p for p in gen_shards[prev_gen] if c in shard_children_map.get(p, [])]
                    assert len(parents_of_c) == 2, \
                        f"Merge child {c} has {len(parents_of_c)} parents, expected 2"

        # Verify that the set of shards with no parent exactly matches
        # root_shard_ids collected earlier (i.e. no orphaned shards).
        shards_without_parents = all_shards - in_children
        assert shards_without_parents == set(root_shard_ids)

        # Walk the shard tree recursively from each root and verify
        # that every root-to-leaf path has length exactly len(tablet_multipliers),
        # confirming one new generation per tablet count change.
        def run_and_verify(shard_id, depth):
            children = shard_children_map.get(shard_id, None)
            if not children:
                assert depth == len(tablet_multipliers)
            else:
                for ch in children:
                    run_and_verify(ch, depth + 1)

        for r in root_shard_ids:
            run_and_verify(r, 1)
    run_parent_children_relationship_test(dynamodb, dynamodbstreams, rest_api, cql, tablet_multipliers, verify_parent_children_relationship)

# this test will:
#  - create a table with streams enabled
#  - get initial tablet count
#  - for each multiplier in tablet_multipliers:
#      - modify tablet count to initial * multiplier - this will trigger
#        creation of new stream shards in the stream as currently we have 1 to 1
#        mapping between tablets and stream shards
#      - perform writes_per_tablet_multiplier writes to the table
#  - after all modifications, use DescribeStream to build shard_parents_map (parent -> child) stream shards map
#  - use GetRecords to read all stream records
#  - verify that:
#      - all written items are present in the stream (check count and then sorted content)
#      - within each partition key, the records are in order of writes (check that `e` is monotonically increasing for each record)
#      - the stream shard parent-child relationships are correct - stream shards create "generations" (all stream shards are split or merged at the same moment,
#        when tablet count is changed). Every stream shard except first ones has a parent.
#        Then we try to walk the tree starting from every stream shard that is not pointed to as a parent. Depending on which generation that stream shard is in,
#        the path will have different length.
def test_get_records_with_alternating_tablets_count(dynamodb, dynamodbstreams, rest_api, cql):
    # Only growing tablet count — merges are incompatible with Alternator Streams.
    tablet_multipliers = [1, 2, 4, 8, 16]
    writes_per_tablet_multiplier = 100
    partition_count = 32

    with create_stream_test_table(dynamodb, StreamViewType='NEW_AND_OLD_IMAGES', Tags=TABLET_TAGS) as table:
        (arn, label) = wait_for_active_stream(dynamodbstreams, table)

        ks = f'alternator_{table.name}'
        table_name = table.name
        cdc_log_table_name = f'{table_name}_scylla_cdc_log'
        init_table_count = get_tablet_count_for_base_table_of_table(rest_api, cql, ks, cdc_log_table_name)

        # --- Phase 1: Drive tablet count changes and write data ---
        # For each multiplier, alter tablet count and write writes_per_tablet_multiplier
        # items to the table. Each write uses a small, bounded set of partition keys
        # (0..partition_count-1) to force per-partition ordering collisions, which
        # lets us later verify that events within a partition appear in write order.
        # `e` is a monotonically increasing counter that encodes the write order.
        index = 0
        expected_items = []
        retrieved_items = []
        for tablet_mult in tablet_multipliers:
            set_tablet_count_and_wait(rest_api, cql, ks, table_name, cdc_log_table_name, init_table_count * tablet_mult)
            for _ in range(0, writes_per_tablet_multiplier):
                p = str(random.randint(0, partition_count - 1))
                index += 1
                # we want to partition keys by small set of partitions to force key collisions
                # to detect any ordering issues within a partition
                c = '1'
                e = str(index)
                table.put_item(Item={'p': p, 'c': c, 'e': e})
                expected_items.append((p, c, e))

        # --- Phase 2: Read all stream records via GetRecords, building shard topology ---
        # Iterate DescribeStream in a retry loop until all expected items have been
        # retrieved from the stream. For each shard discovered:
        #   - Record its parent in shard_parents_map (child -> parent).
        #   - Track root shards (those without a parent).
        #   - Create a GetShardIterator starting at the shard's StartingSequenceNumber
        #     and later call GetRecords to drain it, collecting all the formerly put (p, c, e) triples.
        iterators = {}
        shard_parents_map = {}
        root_shard_ids = []
        end_ts = time.time() + 30
        while len(retrieved_items) < len(expected_items):
            for shard in iterate_over_describe_stream(dynamodbstreams, arn, end_ts):
                shard_id = shard['ShardId']
                parent_shard_id = shard.get('ParentShardId', None)
                if parent_shard_id is None:
                    if shard_id not in root_shard_ids:
                        root_shard_ids.append(shard_id)
                elif shard_id in shard_parents_map:
                    assert shard_parents_map[shard_id] == parent_shard_id
                else:
                    shard_parents_map[shard_id] = parent_shard_id
                if shard_id in iterators:
                    continue
                start = shard['SequenceNumberRange']['StartingSequenceNumber']
                iter = dynamodbstreams.get_shard_iterator(StreamArn=arn, ShardId=shard_id, ShardIteratorType='AT_SEQUENCE_NUMBER',SequenceNumber=start)['ShardIterator']
                assert iter is not None
                iterators[shard_id] = iter


            for shard_id, iter in iterators.items():
                response = dynamodbstreams.get_records(ShardIterator=iter, Limit=1000)
                if 'NextShardIterator' in response:
                    iterators[shard_id] = response['NextShardIterator']

                records = response.get('Records', [])
                for record in records:
                    dynamodb = record['dynamodb']
                    keys = dynamodb['NewImage']
                    
                    assert set(keys) == set(['p', 'c', 'e'])
                    p = keys['p'].get('S')
                    c = keys['c'].get('S')
                    e = keys['e'].get('S')
                    retrieved_items.append((p, c, e))

        # --- Phase 3: Verify completeness and per-partition ordering of stream records ---
        # Check that the set of records retrieved from the stream exactly matches the
        # set of items written (same count, same content when sorted).
        # Then verify that for every partition key `p`, the events appear in the same
        # order they were written: the `e` value (write index) must be strictly
        # increasing across successive reads for the same `p`.
        assert len(retrieved_items) == len(expected_items)
        assert sorted(retrieved_items) == sorted(expected_items)
        previous_values = {}
        # we iterate over retrieved items here in an order of how they were read from the stream
        # implementation might (and will) reorder items for data with different partition keys as it pleases,
        # but for the same partition key the order of events must be preserved.
        # `previous_values` keeps track of last `e` value seen for each partition key `p`. When written for the same `p`
        # next value will always have `e` greater than previous one.
        for p, c, e in retrieved_items:
            e = int(e)
            pv = previous_values.get(p, -1)
            assert pv < e
            previous_values[p] = e
        
        # --- Phase 4: Verify shard topology - root shard count ---
        # The number of root shards (those without a parent) must equal the initial
        # tablet count.
        assert len(root_shard_ids) == init_table_count

        # --- Phase 5: Verify shard topology - generational structure and path lengths ---
        # Build `streams_superseded_by_next_gen`: the set of shard IDs that appear as a parent of some
        # other shard. Shards NOT in this set are "leaves" (the most recent generation).
        #
        # Group all shards (except root ones) by their generation, inferred from the
        # timestamp embedded in the shard ID (characters [2:13]).
        #
        # For each leaf shard, walk the parent chain up to the root (get_path), then
        # assert that the number of siblings in the leaf's generation equals
        # `init_table_count * tablet_multipliers[len(path) - 1]`.
        # This verifies the expected doubling/halving pattern produced by the
        # alternating tablet_multipliers sequence.
        streams_superseded_by_next_gen = set()
        for (shard_id, parent_shard_id) in shard_parents_map.items():
            streams_superseded_by_next_gen.add(parent_shard_id)

        def get_generation_from_shard(t):
            # Shard IDs have the format "H<hex_generation_ts>:<hex_token>", e.g.
            # "H 19b22b8563a:7fffffffffffffffe8547ce46400000"
            # Characters [2:13] extract the hex generation timestamp, which groups
            # shards that were created together during the same tablet count change.
            return t[2:13]
        count_map = {}
        for r in shard_parents_map:
            gen = get_generation_from_shard(r)
            count = count_map.get(gen, 0)
            count_map[gen] = count + 1
        # Returns the ancestor chain from shard_id up to the root (a shard with no parent).
        # The returned list is ordered [shard_id, parent, grandparent, ..., root].
        def get_path(shard_id):
            path = [ shard_id ]
            current_shard_id = shard_id
            while True:
                parent_shard_id = shard_parents_map.get(current_shard_id, None)
                if parent_shard_id is None:
                    return path
                path.append(parent_shard_id)
                current_shard_id = parent_shard_id
        for r in shard_parents_map:
            if r not in streams_superseded_by_next_gen:
                path = get_path(r)
                siblings_count = count_map[get_generation_from_shard(r)]
                assert siblings_count == init_table_count * tablet_multipliers[len(path) - 1]
