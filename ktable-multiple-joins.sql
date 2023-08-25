-- to consume everything from the query
SET 'auto.offset.reset' = 'earliest';

-- input stream with key and values
CREATE STREAM stream_input(
  id_key int KEY, 
  product_id string,
  country_id string
) WITH (
 kafka_topic = 'input_topic', 
 value_format = 'json',
 partitions=1
);

-- create a table 
CREATE TABLE table_country (
  country_id string PRIMARY KEY, 
  country string
) WITH (
 kafka_topic = 'country_topic', 
 value_format = 'json',
 partitions=1
);

INSERT INTO table_country VALUES ('c1', 'italy');
INSERT INTO table_country VALUES ('c2', 'spain');

CREATE TABLE table_product (
  product_id string PRIMARY KEY, 
  product string
) WITH (
 kafka_topic = 'product_topic', 
 value_format = 'json',
 partitions=1
);

INSERT INTO table_product VALUES ('p1', 'pineapple');
INSERT INTO table_product VALUES ('p2', 'pizza');

CREATE STREAM stream_output AS
  SELECT 
    s.id_key KEY,
    s.product_id,
    s.country_id,
    c.country,
    p.product
  FROM stream_input s
    LEFT JOIN table_country c ON s.country_id = c.country_id
    LEFT JOIN table_product p ON s.product_id = p.product_id;

-- this join generates 4 state stores
-- 1 for each table (total 2)
-- 1 for each repartitioning to be able to co-partition the two streams to do the join
-- "Streams allow joins on expressions other than their key column. When the join criteria differ from the KEY column, ksqlDB internally repartitions the stream, which implicitly defines the correct key and partitioning."
--   from https://docs.ksqldb.io/en/latest/developer-guide/joins/partition-data


-- insert values
INSERT INTO stream_input VALUES (1, 'p1', 'c1');
INSERT INTO stream_input VALUES (1, 'p1', 'c2');
INSERT INTO stream_input VALUES (1, 'p2', 'c1');
INSERT INTO stream_input VALUES (1, 'p2', 'c2');