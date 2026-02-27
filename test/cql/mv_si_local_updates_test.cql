-- Simple test case for local materialized views with synchronous local updates.
-- A view is local when its partition key definition is the same as base's,
-- which results in locating view partition on the same node as the base table.
CREATE TABLE t (p1 int, p2 int, c int, v int, primary key ((p1,p2),c));
-- a local view - with identical partition key
CREATE MATERIALIZED VIEW tv AS SELECT * FROM t WHERE p1 IS NOT NULL AND p2 IS NOT NULL AND c IS NOT NULL AND v IS NOT NULL PRIMARY KEY ((p1,p2),v,c);
INSERT INTO t (p1, p2, c, v) VALUES (1,2,3,4);
INSERT INTO t (p1, p2, c, v) VALUES (2,3,4,5);
SELECT * FROM tv;
-- a local index - with underlying local view
CREATE INDEX ON t(v);
INSERT INTO t (p1, p2, c, v) VALUES (3,4,5,6);
INSERT INTO t (p1, p2, c, v) VALUES (4,5,6,6);
SELECT * FROM t WHERE v = 6;
