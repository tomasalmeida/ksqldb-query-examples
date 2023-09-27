---- Proposal
-- given a stream in avro format, convert it to a string

-- to consume everything from the query
SET 'auto.offset.reset' = 'earliest';

CREATE STREAM PERSON (
        ID STRING KEY,
        AGE INT,
        NAME STRING,
        SURNAME STRING
    )
    WITH (
        KAFKA_TOPIC='person',
        PARTITIONS=1,
        VALUE_FORMAT='AVRO'
    );


CREATE STREAM PERSON_STRING
    WITH (
        KAFKA_TOPIC='person-string',
        VALUE_FORMAT='KAFKA'
    )
AS SELECT
    ID,
    CONCAT('{"AGE":"',         CAST(AGE AS STRING), 
           '","FULL_NAME": "', NAME, ' ', SURNAME, '"}') 
        AS VALUE
FROM PERSON
EMIT CHANGES;



INSERT INTO PERSON (ID, AGE, NAME, SURNAME) VALUES ('1', 10, 'Alex', 'Doe');
INSERT INTO PERSON (ID, AGE, NAME, SURNAME) VALUES ('2', 20, 'Jon', 'Smith');
INSERT INTO PERSON (ID, AGE, NAME, SURNAME) VALUES ('3', 30, 'Sven', 'Laplace');