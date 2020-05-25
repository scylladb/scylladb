CREATE KEYSPACE k WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
USE k;
CREATE TABLE error_name (name ascii PRIMARY KEY, one_shot ascii);
INSERT INTO  error_name (name, one_shot) VALUES ('test1', 'true');
INSERT INTO  error_name (name, one_shot) VALUES ('test2', 'false');
SELECT enable_injection(name, one_shot)  FROM error_name WHERE name = 'test1';
SELECT enable_injection(name, one_shot)  FROM error_name WHERE name = 'test2';
SELECT disable_injection(name) FROM error_name WHERE name = 'test1';
SELECT enabled_injections()    FROM error_name LIMIT 1;
