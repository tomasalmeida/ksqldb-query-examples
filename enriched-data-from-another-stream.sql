-- to consume everything from the query
SET 'auto.offset.reset' = 'earliest';

-- input stream A and B
CREATE STREAM stream_a(
  value1 string,
  value2 string
) WITH (
 kafka_topic = 'topic_a', 
 value_format = 'json',
 partitions = 1
);

CREATE STREAM stream_b(
  value1 string,
  value2 string,
  value3 string,
  value4 string
) WITH (
 kafka_topic = 'topic_b', 
 value_format = 'json',
 partitions = 1
);


-- input data
INSERT INTO stream_a VALUES ('v1', 'v1');
INSERT INTO stream_b VALUES ('v1', 'v1', 'v1-c', 'v1-d');
INSERT INTO stream_a VALUES ('v2', 'v2');
INSERT INTO stream_a VALUES ('v3', 'v3');
INSERT INTO stream_b VALUES ('v2', 'v2', 'v2-c', 'v2-d');
INSERT INTO stream_b VALUES ('v4', 'v4', 'v4-c', 'v4-d');

-- create another stream with key concatenated
CREATE STREAM stream_a_concat AS
 SELECT
    a.value1 + '-' + a.value2 as a_key,
    a.value1,
    a.value2
  FROM stream_a a;


CREATE STREAM stream_b_concat AS
 SELECT
    b.value1 + '-' + b.value2 as b_key,
    b.value3,
    b.value4
  FROM stream_b b;

-- repartition by key
CREATE STREAM stream_a_key AS
 SELECT *
 FROM stream_a_concat ac
 PARTITION BY ac.a_key;

CREATE STREAM stream_b_key AS
 SELECT *
 FROM stream_b_concat bc
 PARTITION BY bc.b_key;


-- join the result
CREATE STREAM stream_final AS
SELECT 
    ak.a_key,
    ak.value1,
    ak.value2,
    bk.value3,
    bk.value4
  FROM stream_a_key ak
    INNER JOIN stream_b_key bk
      WITHIN 30 DAYS
      GRACE PERIOD 1 DAY
      ON ak.a_key = bk.b_key;


-- CHECK the result
SELECT * FROM stream_final EMIT CHANGES;

-- In another ksql cli add more data
INSERT INTO stream_a VALUES ('v5', 'v5');
INSERT INTO stream_b VALUES ('v5', 'v5', 'v5-c', 'v5-d');
INSERT INTO stream_a VALUES ('v6', 'v6');
INSERT INTO stream_a VALUES ('v7', 'v7');
INSERT INTO stream_b VALUES ('v6', 'v6', 'v6-c', 'v6-d');
INSERT INTO stream_b VALUES ('v8', 'v8', 'v8-c', 'v8-d');