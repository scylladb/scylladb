-- CQL setup for auth workload
DROP KEYSPACE IF EXISTS auth_test;
CREATE KEYSPACE auth_test WITH replication = {'class':'NetworkTopologyStrategy','replication_factor':1};
USE auth_test;
CREATE TABLE auth_table (pk int PRIMARY KEY, v text);

-- Insert a few rows
INSERT INTO auth_table (pk, v) VALUES (1, 'one');
INSERT INTO auth_table (pk, v) VALUES (2, 'two');
INSERT INTO auth_table (pk, v) VALUES (3, 'three');

-- Create roles
CREATE ROLE IF NOT EXISTS parent_role;
CREATE ROLE IF NOT EXISTS child_role;
CREATE ROLE IF NOT EXISTS reader_role WITH PASSWORD = 'password1' AND LOGIN = true;

-- Role hierarchy (parent<-child<-reader)
GRANT parent_role TO child_role;
GRANT child_role TO reader_role;

-- Grant permissions
GRANT MODIFY ON auth_test.auth_table TO parent_role;
GRANT SELECT ON auth_test.auth_table TO parent_role;
