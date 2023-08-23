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

CREATE STREAM stream_final AS
  SELECT 
    t.id,
    t.value1,
    i.value5
  FROM stream2_input i
    LEFT JOIN table_input t
      ON i.id = t.id
  WHERE t.id > 0;

INSERT INTO stream2_input VALUES (5, 'val5');
