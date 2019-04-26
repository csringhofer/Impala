CREATE DATABASE IF NOT EXISTS acid;
USE acid;


DROP TABLE IF EXISTS insert_only_no_partitions;
CREATE TABLE insert_only_no_partitions (expected_write_id int, description string)
  TBLPROPERTIES ('transactional'='true', 'transactional_properties'='insert_only');

INSERT INTO insert_only_no_partitions VALUES (1, 'before compaction 1');
INSERT INTO insert_only_no_partitions VALUES (2, 'before compaction 1');

ALTER TABLE insert_only_no_partitions COMPACT 'major' AND WAIT;

INSERT INTO insert_only_no_partitions VALUES (3, 'between compaction 1 and 2');
INSERT INTO insert_only_no_partitions VALUES (4, 'between compaction 1 and 2');

ALTER TABLE  insert_only_no_partitions COMPACT 'major' AND WAIT;

INSERT INTO insert_only_no_partitions VALUES (5, 'after compaction 2');
INSERT INTO insert_only_no_partitions VALUES (6, 'after compaction 2');

DROP TABLE IF EXISTS insert_only_with_partitions;
CREATE TABLE insert_only_with_partitions (expected_write_id int, description string) PARTITIONED BY (p string)
  TBLPROPERTIES ('transactional'='true', 'transactional_properties'='insert_only');


INSERT INTO insert_only_with_partitions PARTITION (p='compacted') VALUES (1, 'before compaction 1');
INSERT INTO insert_only_with_partitions PARTITION (p='compacted') VALUES (2, 'before compaction 1');
INSERT INTO insert_only_with_partitions PARTITION (p='not_compacted') VALUES (3, 'not compacted');

ALTER TABLE insert_only_with_partitions PARTITION (p='compacted') COMPACT 'major' AND WAIT;

INSERT INTO insert_only_with_partitions PARTITION (p='compacted') VALUES (4, 'between compaction 1 and 2');
INSERT INTO insert_only_with_partitions PARTITION (p='compacted') VALUES (5, 'between compaction 1 and 2');
INSERT INTO insert_only_with_partitions PARTITION (p='not_compacted') VALUES (6, 'not compacted');

ALTER TABLE insert_only_with_partitions PARTITION (p='compacted') COMPACT 'major' AND WAIT;

INSERT INTO insert_only_with_partitions PARTITION (p='compacted') VALUES (7, 'after compaction 2');
INSERT INTO insert_only_with_partitions PARTITION (p='compacted') VALUES (8, 'after compaction 2');
INSERT INTO insert_only_with_partitions PARTITION (p='not_compacted') VALUES (9, 'not compacted');



