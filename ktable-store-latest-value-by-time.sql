-- to consume everything from the query
SET 'auto.offset.reset' = 'earliest';

-- input stream with key, value and time
CREATE STREAM stream_input(
  key int, 
  value string,
  time timestamp
) WITH (
 kafka_topic = 'stream_input', 
 value_format = 'json',
 partitions=1
);

-- create a table to get the element with biggest time
-- use unix_timestamp to be able to get max (using int instead of timestamp)
CREATE TABLE max_by_time AS
  SELECT key, MAX(UNIX_TIMESTAMP(time)) AS time_max 
  FROM stream_input 
  GROUP BY key
EMIT CHANGES;

-- create a stream joining the stream with a table
-- get the value based on key and the latest value
-- and insert in another stream
CREATE STREAM stream_latest_value AS
  SELECT 
    s.key as key, 
    s.value
  FROM stream_input s
  LEFT JOIN max_by_time t
    ON s.key = t.key
  WHERE 
    UNIX_TIMESTAMP(s.time) >= t.time_max OR 
    t.time_max is null
EMIT CHANGES;
 
-- create another table to materialise the results
CREATE TABLE values_table AS
SELECT 
   key, 
   LATEST_BY_OFFSET(value) AS value
FROM stream_latest_value 
GROUP BY key
EMIT CHANGES;
 
-- get the table updates 
SELECT * FROM values_table EMIT CHANGES;

-- in another ksql cli insert values and observe the values
INSERT INTO stream_input VALUES (1, 'ONE',           '2022-08-01T10:00');
INSERT INTO stream_input VALUES (2, 'TWO',           '2022-08-01T10:00');
INSERT INTO stream_input VALUES (3, 'THREE',         '2022-08-01T10:00');
INSERT INTO stream_input VALUES (4, 'FOUR',          '2022-08-01T10:00');
INSERT INTO stream_input VALUES (5, 'FIVE',          '2022-08-01T10:00');
INSERT INTO stream_input VALUES (5, 'FIVE_MEGA',     '2022-08-01T11:00');
INSERT INTO stream_input VALUES (5, 'FIVE',          '2022-08-01T09:00');
INSERT INTO stream_input VALUES (5, 'FIVE_min',      '2022-08-01T00:00');
INSERT INTO stream_input VALUES (5, 'FIVE_MEGA_MAX', '2022-08-01T23:59');
INSERT INTO stream_input VALUES (5, 'FIVE',          '2022-08-01T10:00');
