create table foo (a int, b int, c int, PRIMARY KEY (a, b, c)) WITH CLUSTERING ORDER BY (b DESC, c ASC);

INSERT INTO foo (a, b, c) VALUES (0, 2, 0);
INSERT INTO foo (a, b, c) VALUES (0, 1, 0);
INSERT INTO foo (a, b, c) VALUES (0, 1, 1);
INSERT INTO foo (a, b, c) VALUES (0, 0, 0);

SELECT * FROM foo WHERE a=0 AND (b, c) > (1, 0);

SELECT * FROM foo WHERE a=0 AND (b, c) > SCYLLA_CLUSTERING_BOUND (1, 0);

SELECT * FROM foo WHERE a=0 AND (b, c) > (2, 0);

SELECT * FROM foo WHERE a=0 AND (b, c) > SCYLLA_CLUSTERING_BOUND (2, 0);

SELECT * FROM foo WHERE a=0 AND (b, c) >= (2, 0);

SELECT * FROM foo WHERE a=0 AND (b, c) >= SCYLLA_CLUSTERING_BOUND (2, 0);

SELECT * FROM foo WHERE a=0 AND (b, c) < (1, 0);

SELECT * FROM foo WHERE a=0 AND (b, c) < SCYLLA_CLUSTERING_BOUND (1, 0);

SELECT * FROM foo WHERE a=0 AND (b, c) < (2, 0);

SELECT * FROM foo WHERE a=0 AND (b, c) < SCYLLA_CLUSTERING_BOUND (2, 0);

SELECT * FROM foo WHERE a=0 AND (b, c) <= (2, 0);

SELECT * FROM foo WHERE a=0 AND (b, c) <= SCYLLA_CLUSTERING_BOUND (2, 0);


SELECT * FROM foo WHERE a=0 AND (b) > (1);

SELECT * FROM foo WHERE a=0 AND (b) > SCYLLA_CLUSTERING_BOUND (1);

SELECT * FROM foo WHERE a=0 AND (b) > (2);

SELECT * FROM foo WHERE a=0 AND (b) > SCYLLA_CLUSTERING_BOUND (2);

SELECT * FROM foo WHERE a=0 AND (b) >= (2);

SELECT * FROM foo WHERE a=0 AND (b) >= SCYLLA_CLUSTERING_BOUND (2);

SELECT * FROM foo WHERE a=0 AND (b) < (1);

SELECT * FROM foo WHERE a=0 AND (b) < SCYLLA_CLUSTERING_BOUND (1);

SELECT * FROM foo WHERE a=0 AND (b) < (2);

SELECT * FROM foo WHERE a=0 AND (b) < SCYLLA_CLUSTERING_BOUND (2);

SELECT * FROM foo WHERE a=0 AND (b) <= (2);

SELECT * FROM foo WHERE a=0 AND (b) <= SCYLLA_CLUSTERING_BOUND (2);




SELECT * FROM foo WHERE a=0 AND (b) > (1) AND (b, c) <= (2, 0);

SELECT * FROM foo WHERE a=0 AND (b) > SCYLLA_CLUSTERING_BOUND (1) AND (b, c) <= SCYLLA_CLUSTERING_BOUND (0, 0);

SELECT * FROM foo WHERE a=0 AND (b) > (2) AND (b, c) <= (2, 1);

SELECT * FROM foo WHERE a=0 AND (b) > SCYLLA_CLUSTERING_BOUND (2) AND (b, c) <= SCYLLA_CLUSTERING_BOUND(1, 1);

-- error checks --

-- wrong side of condition
SELECT * FROM foo WHERE a=0 AND SCYLLA_CLUSTERING_BOUND(b) > (2);

-- both sides of condition
SELECT * FROM foo WHERE a=0 AND SCYLLA_CLUSTERING_BOUND(b) > SCYLLA_CLUSTERING_BOUND(2);

-- too many values --
SELECT * FROM foo WHERE a=0 AND (b, c) > SCYLLA_CLUSTERING_BOUND (1, 0, 5);

-- too few values --
SELECT * FROM foo WHERE a=0 AND (b, c) > SCYLLA_CLUSTERING_BOUND (1);

-- missing values --
SELECT * FROM foo WHERE a=0 AND (b, c) > SCYLLA_CLUSTERING_BOUND;

-- not tuple  --
SELECT * FROM foo WHERE a=0 AND (b, c) > SCYLLA_CLUSTERING_BOUND 45;

-- just wrong  --
SELECT * FROM foo WHERE a=0 SCYLLA_CLUSTERING_BOUND AND (b, c) > (0, 1);
SELECT * FROM foo WHERE a=0 AND (b, c) > (0, 1) SCYLLA_CLUSTERING_BOUND;

-- mixing apples and make_count_rows_function
SELECT * FROM foo WHERE a=0 AND (b, c) > SCYLLA_CLUSTERING_BOUND(2, 0)  AND (b, c) < (1, 1);
-- and again --
SELECT * FROM foo WHERE a=0 AND (b, c) > SCYLLA_CLUSTERING_BOUND(2, 0)  AND (b, c) < (2, 0);
