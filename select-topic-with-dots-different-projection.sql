-- create using Control center (http://localhost:9021) the topic "topic_underscore.dots"

-- set an schema for the topic
{
  "type": "record",
  "namespace": "com.mycorp.mynamespace",
  "name": "sampleRecord",
  "doc": "Sample schema to help you get started.",
  "fields": [
    {
      "name": "f1",
      "type": "int"
    },
    {
      "name": "f2",
      "type": "string"
    },
    {
      "name": "f3",
      "type": "string"
    }
  ]
}

-- produce some data
kafka-avro-console-producer --bootstrap-server localhost:9092 --topic topic_underscore.dots --property schema.registry.url=http://localhost:8081 --property value.schema.id=1
{ "f1": 1, "f2" : "one", "f3" : "one" }
{ "f1": 2, "f2" : "two", "f3" : "two" }


-- consume the data
kafka-avro-console-consumer --topic topic_underscore.dots --from-beginning --bootstrap-server localhost:9092 --property schema.registry.url=http://localhost:8081 --property print.schema.ids=true

-- create the stream
CREATE STREAM NEW_NAME (
    f1 INT,
    f2 STRING
)
WITH (
    KAFKA_TOPIC='topic_underscore.dots',
    VALUE_FORMAT='AVRO'
);

CREATE STREAM NEW_NAME2 (
    f1 INT,
    f2 STRING,
    f3 STRING
)
WITH (
    KAFKA_TOPIC='topic_underscore.dots',
    VALUE_FORMAT='AVRO'
);

-- check results
select * FROM NEW_NAME;
select * from NEW_NAME where f1 = 1;

select * FROM NEW_NAME2;
select * from NEW_NAME2 where f1 = 1;