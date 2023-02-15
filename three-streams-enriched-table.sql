-- to consume everything from the query
SET 'auto.offset.reset' = 'earliest';

-- input stream A, B and C
CREATE OR REPLACE STREAM stream_a(
  valueA1 STRING,
  valueA2 STRING,
  common STRING
) WITH (
 kafka_topic = 'topic_a', 
 value_format = 'avro',
 partitions = 1
);

CREATE STREAM stream_b(
  valueB1 STRING,
  valueB2 STRING,
  common STRING
) WITH (
 kafka_topic = 'topic_b', 
 value_format = 'avro',
 partitions = 1
);

CREATE STREAM stream_c(
  valueC1 STRING,
  valueC2 STRING,
  common STRING
) WITH (
 kafka_topic = 'topic_c', 
 value_format = 'avro',
 partitions = 1
);

-- confirm streams are created
SHOW STREAMS;


-- input data
INSERT INTO stream_a VALUES ('a1', 'a1', '1');
INSERT INTO stream_a VALUES ('a2', 'a2', '2');
INSERT INTO stream_b VALUES ('b1', 'b1', '1');
INSERT INTO stream_b VALUES ('b2', 'b2', '2');
INSERT INTO stream_c VALUES ('c1', 'c1', '1');
INSERT INTO stream_c VALUES ('c2', 'c2', '2');

--  DROP TABLE TABLE_RICH;

-- # this large version create a stream and a table from the stream

-- CREATE STREAM stream_rich AS
--     SELECT
--         sa.common as common,
--         sa.valueA1,
--         sa.valueA2,
--         sb.valueB1,
--         sb.valueB2,
--         sc.valueC1,
--         sc.valueC2
--     FROM stream_a sa
--         LEFT JOIN stream_b sb WITHIN 30 SECONDS ON sa.common = sb.common
--         LEFT JOIN stream_c sc WITHIN 30 SECONDS ON sa.common = sc.common
--     PARTITION BY sa.common
--     EMIT CHANGES;

--  CREATE TABLE table_rich AS
--     SELECT
--         common,
--         LATEST_BY_OFFSET(sa.valueA1) as A1,
--         LATEST_BY_OFFSET(sa.valueA2) as A2,
--         LATEST_BY_OFFSET(sb.valueB1) as B1,
--         LATEST_BY_OFFSET(sb.valueB2) as B2,
--         LATEST_BY_OFFSET(sc.valueC1) as C1,
--         LATEST_BY_OFFSET(sc.valueC2) as C2
--     FROM stream_rich
--     GROUP BY common
--     HAVING count(*) > 0;

 CREATE TABLE table_rich2 AS
    SELECT
       sa.common,
       LATEST_BY_OFFSET(sa.valueA1) as A1,
       LATEST_BY_OFFSET(sa.valueA2) as A2,
       LATEST_BY_OFFSET(sb.valueB1) as B1,
       LATEST_BY_OFFSET(sb.valueB2) as B2,
       LATEST_BY_OFFSET(sc.valueC1) as C1,
       LATEST_BY_OFFSET(sc.valueC2) as C2
    FROM stream_a sa
        LEFT JOIN stream_b sb WITHIN 30 SECONDS ON sa.common = sb.common
        LEFT JOIN stream_c sc WITHIN 30 SECONDS ON sa.common = sc.common
    GROUP BY sa.common
    HAVING count(*) > 0;

-- tests
select * from table_rich emit changes;

INSERT INTO stream_a VALUES ('a3', 'a3', '3');
INSERT INTO stream_b VALUES ('b3', 'b3', '3');
INSERT INTO stream_c VALUES ('c3', 'c3', '3');

INSERT INTO stream_a VALUES ('a4', 'a4', '4');
INSERT INTO stream_c VALUES ('c4', 'c4', '4');
INSERT INTO stream_b VALUES ('b4', 'b4', '4');

INSERT INTO stream_c VALUES ('c5', 'c5', '5');
INSERT INTO stream_b VALUES ('b5', 'b5', '5');
INSERT INTO stream_a VALUES ('a5', 'a5', '5');
