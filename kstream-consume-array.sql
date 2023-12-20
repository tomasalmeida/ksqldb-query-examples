-- to consume everything from the query
SET 'auto.offset.reset' = 'earliest';

CREATE STREAM stream_input(
  key int, 
  value ARRAY<
            STRUCT<
                f1 STRING, 
                f2 ARRAY<STRING>
            >
        >
) WITH (
 kafka_topic = 'stream_input', 
 value_format = 'json',
 partitions=1
);

-- insert events (I used Control center to add the object)
{
	"key": 1,
	"value": [
		{
            "f1": "a1",
            "f2": ["A11","B11"]
		},
		{
            "f1": "b1",
            "f2": ["A12","B12"]
        }
	]
}

-- insert events (I used Control center to add the object)
{
	"key": 2,
	"value": [
		{
		    "f1": "a2",
		    "f2": ["A21","B21"]
		},
		{
		    "f1": "b2",
		    "f2": ["A22","B22"]
		}
	]
}



CREATE STREAM exploded_stream as 
	SELECT EXPLODE(value)->f2 FROM stream_input;


select explode(f2) FROM exploded_stream;