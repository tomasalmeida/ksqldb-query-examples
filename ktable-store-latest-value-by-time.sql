-- to consume everything from the query
SET 'auto.offset.reset' = 'earliest';

-- input stream with key, value and "time" 
-- for simplicity time is a int value
CREATE STREAM stream_input(
  key int, 
  value string,
  time int
) WITH (
 kafka_topic = 'stream1', 
 value_format = 'json',
 partitions=1
);

-- create a table to get the element with biggest time
CREATE TABLE max_by_time AS
  SELECT key, MAX(time) AS time_max 
  FROM stream_input 
  GROUP BY key
EMIT CHANGES;

-- create a stream joining the stream with a table
-- get the value based on key and the latest value
-- and insert in another stream
CREATE STREAM max_values AS
  SELECT 
    s.key as key, 
    s.value
  FROM stream_input s
  LEFT JOIN max_by_time t
    ON s.key = t.key
  WHERE 
    s.time >= t.time_max OR 
    t.time_max is null
EMIT CHANGES;
 
-- create another table to materialise the results
CREATE TABLE max_values_table AS
SELECT 
   key, 
   LATEST_BY_OFFSET(value)
FROM max_values 
GROUP BY key
EMIT CHANGES;
 
-- get the table updates 
SELECT * FROM max_values_table EMIT CHANGES;

-- in another ksql cli insert values and observe the values
INSERT INTO stream_input VALUES (1,'ONE',11);
INSERT INTO stream_input VALUES (2,'TWO',22);
INSERT INTO stream_input VALUES (3,'THREE',31);
INSERT INTO stream_input VALUES (4,'FOUR',41);
INSERT INTO stream_input VALUES (5,'FIVE',51);
INSERT INTO stream_input VALUES (5,'FIVE_MEGA',101);
INSERT INTO stream_input VALUES (5,'FIVE',61);
INSERT INTO stream_input VALUES (5,'FIVE_min',11);
INSERT INTO stream_input VALUES (5,'FIVE_MEGA_MAX',1111);
INSERT INTO stream_input VALUES (5,'FIVE',41);
