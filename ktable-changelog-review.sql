-- to consume everything from the query
SET 'auto.offset.reset' = 'earliest';

-- input stream with key and values
CREATE STREAM stream_input(
  id int KEY, 
  value1 string,
  value2 string,
  value3 string,
  value4 string
) WITH (
 kafka_topic = 'input_topic', 
 value_format = 'json',
 partitions=1
);

-- insert values
INSERT INTO stream_input VALUES (1, 'val1', 'val2', 'val3', 'val4');
INSERT INTO stream_input VALUES (2, 'val1', 'val2', 'val3', 'val4');
INSERT INTO stream_input VALUES (3, 'val1', 'val2', 'val3', 'val4');
INSERT INTO stream_input VALUES (4, 'val1', 'val2', 'val3', 'val4');
INSERT INTO stream_input VALUES (5, 'val1', 'val2', 'val3', 'val4');
INSERT INTO stream_input VALUES (5, 'val1', 'val2', 'val3', 'val4');
INSERT INTO stream_input VALUES (5, 'val1', 'val2', 'val3', 'val4');
INSERT INTO stream_input VALUES (5, 'val1', 'val2', 'val3', 'val4');
INSERT INTO stream_input VALUES (5, 'val1', 'val2', 'val3', 'val4');
INSERT INTO stream_input VALUES (5, 'val1', 'val2', 'val3', 'val4');

-- create a table 
CREATE TABLE table_input (
  id int PRIMARY KEY, 
  value1 string,
  value2 string,
  value3 string,
  value4 string
) WITH (
 kafka_topic = 'input_topic', 
 value_format = 'json',
 partitions=1
);

CREATE STREAM stream2_input(
  id int KEY, 
  value5 string
) WITH (
 kafka_topic = 'input_topic2', 
 value_format = 'json',
 partitions=1
);


-- this stream generates a state store with all values from table_input (value 1 to 4)
CREATE STREAM stream_final AS
  SELECT 
    t.id,
    CONCAT(CONCAT(CAST(t.id AS STRING), '-'), s.value5) _key,
    t.value1,
    s.value5
  FROM stream2_input s
    LEFT JOIN table_input t
      ON s.id = t.id
    PARTITION BY CONCAT(CONCAT(CAST(t.id AS STRING), '-'), s.value5)
;

-- this stream generates ANOTHER state store with all values from table_input (value 1 to 4)
CREATE STREAM stream2_final AS
  SELECT 
    t.id,
    t.value1,
    s.value5
  FROM stream2_input s
    LEFT JOIN table_input t
      ON s.id = t.id
;

INSERT INTO stream2_input VALUES (5, 'val5');
