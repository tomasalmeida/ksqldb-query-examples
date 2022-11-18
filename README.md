# ksqldb-query-examples
Some sample queries with ksqlDB

* [Ktable to get the latest value based on another field timestamp](ktable-store-latest-value-by-time.sql)
* [Get stream with enriched data from another stream](enriched-data-from-another-stream.sql)


## How to run

### Cluster

```shell
    docker-compose up -d
```

### Ksqldb cli

```shell
    docker exec -it ksqldb-cli ksql http://ksqldb-server:8088
```


### Rest API calls

Load file (ktable-store-latest-value-by-time.sql)

```
    curl -X "POST" "http://localhost:8088/query" \
        -H "Accept: application/vnd.ksql.v1+json" \
        -d $'{
    "ksql": "SELECT * FROM values_table where key=1;",
    "streamsProperties": {}
    }' | jq
```

```
    curl -X "POST" "http://localhost:8088/query" \
        -H "Accept: application/vnd.ksql.v1+json" \
        -d $'{
    "ksql": "SELECT * FROM values_table EMIT CHANGES;",
    "streamsProperties": {}
    }'
```