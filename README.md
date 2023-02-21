# Real-time Data Processing and Analysis with Kafka, Connect, KSQL, Elasticsearch, and Flask



## Architectural overview

![Architecture](/docs/kafka.png)




# Get started

## Prerequisites & setup
- install docker/docker-compose
- clone this repo!

## Docker Startup
```
docker-compose up -d
```


# KSQL

Go control-center:
```
localhost:9021
```

Run the KSQL script: 

```
scripts/03_ksql.ksql';
exit;
```

# Load Dynamic Templates for Elastic
```
./04_elastic_dynamic_template
```

# Setup Kafka Connect Elastic Sink
Write topic `runner_status` and `runner_location` to Elastic
```
./05_kafka_to_elastic_sink
```

Check connector status: 

```
curl -s "http://localhost:8083/connectors?expand=info&expand=status" | \
         jq '. | to_entries[] | [ .value.info.type, .key, .value.status.connector.state,.value.status.tasks[].state,.value.info.config."connector.class"]|join(":|:")' | \
         column -s : -t| sed 's/\"//g'| sort
```



## Flask-Api

- Open http://localhost:5000/