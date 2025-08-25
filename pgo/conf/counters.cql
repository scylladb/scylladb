DROP KEYSPACE IF EXISTS counters;

-- FIXME: use tablets after https://github.com/scylladb/scylladb/issues/18180 is done.
CREATE KEYSPACE IF NOT EXISTS counters WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': '3'} AND TABLETS = {'enabled': false};

CREATE TABLE IF NOT EXISTS counters.counter1 (key blob PRIMARY KEY, "C0" counter, "C1" counter, "C2" counter, "C3" counter, "C4" counter);
