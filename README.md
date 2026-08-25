# Real-Time User Events Streaming Data Pipeline

An end-to-end **real-time data engineering pipeline** that ingests user event data from a REST API, streams it through **Apache Kafka**, processes it with **Apache Spark Structured Streaming**, and stores it in **Apache Cassandra**. The whole workflow is scheduled by **Apache Airflow** and runs on **Docker** containers.

```
Random User API  ->  Airflow  ->  Kafka  ->  Spark Streaming  ->  Cassandra
```

---

## Use Case

Companies that sign up new users, such as banks, retailers and SaaS products, need those sign-up events available for analytics within seconds. A nightly batch job is too slow for fraud checks, welcome emails or live dashboards.

This project builds that path. It continuously pulls new user records from an API, moves them through a message queue, cleans and reshapes them in flight, and writes them to a database built for fast writes and high availability.

The [randomuser.me](https://randomuser.me/) API stands in for a real sign-up service, so the pipeline can be run and tested by anyone without private data.

---

## Data Transformation

### Before: raw API response

The source returns one user per request as **nested JSON**, roughly 1 KB per record. Many fields are not needed, and the useful values sit two or three levels deep.

```json
{
  "gender": "female",
  "name": { "title": "Miss", "first": "Anna", "last": "Olsen" },
  "location": {
    "street": { "number": 4555, "name": "Fasanvænget" },
    "city": "Kongsvinger",
    "state": "Danmark",
    "country": "Denmark",
    "postcode": 32986,
    "coordinates": { "latitude": "-43.8562", "longitude": "126.6238" },
    "timezone": { "offset": "-9:00", "description": "Alaska" }
  },
  "email": "anna.olsen@example.com",
  "login": {
    "uuid": "1a017261-433a-47b2-9040-81e883c3a884",
    "username": "beautifullion198",
    "password": "crave",
    "salt": "mnOERh2a",
    "md5": "afe0b9d6c56be8953a14ac4b9af57619"
  },
  "dob": { "date": "1997-05-06T21:32:55.230Z", "age": 29 },
  "registered": { "date": "2012-07-01T22:48:42.393Z", "age": 14 },
  "phone": "27265523",
  "cell": "14990494",
  "id": { "name": "CPR", "value": "060597-4765" },
  "picture": {
    "large": "https://randomuser.me/api/portraits/women/48.jpg",
    "medium": "https://randomuser.me/api/portraits/med/women/48.jpg"
  },
  "nat": "DK"
}
```

### After: cleaned record in Cassandra

The pipeline keeps only the useful fields, flattens the nesting and assigns a unique ID. This is what a stored row looks like:

```json
{
  "id": "b2f4c1e6-902c-4480-a427-1dc4b24b3ccc",
  "first_name": "Anna",
  "last_name": "Olsen",
  "gender": "female",
  "address": "4555 Fasanvænget, Kongsvinger, Danmark, Denmark",
  "post_code": 32986,
  "email": "anna.olsen@example.com",
  "username": "beautifullion198",
  "registered_date": "2012-07-01T22:48:42.393Z",
  "phone": "27265523",
  "picture": "https://randomuser.me/api/portraits/med/women/48.jpg"
}
```

### Field mapping

| Transformation | Before | After |
|---|---|---|
| Combine name parts | `name.first`, `name.last` | `first_name`, `last_name` |
| Build a single address string | `location.street.number`, `location.street.name`, `location.city`, `location.state`, `location.country` | `address` |
| Lift nested values | `login.username`, `registered.date`, `picture.medium` | `username`, `registered_date`, `picture` |
| Generate a primary key | none in source | `id` (UUID) |
| Drop unused fields | `password`, `salt`, `md5`, `coordinates`, `timezone`, `cell`, `nat` | removed |

---

## How the data flows

| Step | What happens | Technology |
|------|--------------|------------|
| 1 | An Airflow DAG calls the API and flattens the nested JSON into clean fields | Airflow, Python, `requests` |
| 2 | Each record is published as a JSON message to the `users_created` Kafka topic | Kafka producer (`kafka-python`) |
| 3 | Spark reads the topic as a continuous stream | Spark Structured Streaming |
| 4 | Spark applies a schema, parses the JSON and selects the needed columns | Spark SQL, `StructType` |
| 5 | Rows are written to Cassandra as they arrive | Spark Cassandra Connector |

---

## Tech Stack

| Layer | Tool | Version |
|-------|------|---------|
| Workflow Orchestration | Apache Airflow | 2.6.0 |
| Message Broker | Apache Kafka (Confluent Platform) | 7.4.0 |
| Coordination | Zookeeper | 7.4.0 |
| Schema Management | Confluent Schema Registry | 7.4.0 |
| Monitoring | Confluent Control Center | 7.4.0 |
| Stream Processing | Apache Spark (Structured Streaming) | 3.4.1 |
| NoSQL Database | Apache Cassandra | latest |
| Metadata Database | PostgreSQL | 14.0 |
| Containerization | Docker and Docker Compose | N/A |
| Language | Python | 3.11 |

---

## Data Model

Events land in the `spark_streams.created_users` table in Cassandra:

```sql
CREATE TABLE spark_streams.created_users (
    id UUID PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    gender TEXT,
    address TEXT,
    post_code TEXT,
    email TEXT,
    username TEXT,
    registered_date TEXT,
    phone TEXT,
    picture TEXT
);
```

`id` is the partition key, so writes spread evenly across the cluster and lookups by user ID are fast.

---

## Project Structure

```
.
├── dags/
│   └── kafka_stream.py      # Airflow DAG: API to Kafka producer
├── script/
│   └── entrypoint.sh        # Airflow container init (DB setup, admin user, deps)
├── spark_stream.py          # Spark job: Kafka to transform to Cassandra
├── docker-compose.yml       # All 10 services defined here
├── requirements.txt         # Python dependencies
└── README.md
```

---

## Getting Started

### Prerequisites

- Docker Desktop, with at least **8 GB RAM** allocated, since the stack runs several JVM services
- Python 3.11
- Java 8, 11 or 17, required by Spark

### 1. Start the infrastructure

```bash
docker compose up -d
```

This launches Zookeeper, Kafka, Schema Registry, Control Center, Airflow (webserver and scheduler), PostgreSQL, Spark (master and worker), and Cassandra.

### 2. Set up the Python environment

```bash
python3.11 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 3. Produce events to Kafka

Either trigger the `user_automation` DAG from the Airflow UI at http://localhost:8080 (login `admin` / `admin`), or run the producer directly:

```bash
python dags/kafka_stream.py
```

### 4. Start the Spark streaming job

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 1.8)
python spark_stream.py
```

### 5. Verify the results

```bash
docker exec -it cassandra cqlsh -e "SELECT * FROM spark_streams.created_users LIMIT 5;"
```

---

## Service Endpoints

| Service | Endpoint | Type |
|---------|----------|------|
| Airflow UI | http://localhost:8080 | Web UI |
| Kafka Control Center | http://localhost:9021 | Web UI |
| Spark Master UI | http://localhost:9090 | Web UI |
| Schema Registry | http://localhost:8081 | REST API |
| Cassandra | localhost:9042 | CQL, connect with cqlsh |

---

## Key Concepts Demonstrated

- **Real-time streaming ingestion** 
- **Decoupled architecture**, so the producer and consumer scale independently through Kafka
- **Distributed processing** with a Spark master and worker cluster
- **Workflow orchestration** using Airflow DAGs with scheduling and retries
- **Schema enforcement** on streaming JSON with Spark `StructType`
- **Checkpointing** for fault tolerance, so the job resumes from the last processed offset after a restart
- **NoSQL data modeling** for a write-heavy workload
- **Infrastructure as code**, with the entire ten-service stack defined in one Compose file

---

## Configuration

Hostnames differ depending on where the code runs. Both cases are supported through environment variables:

| Variable | Default (host) | Inside Docker |
|----------|----------------|---------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | `broker:29092` |
| `CASSANDRA_HOST` | `localhost` | `cassandra` |
| `SPARK_MASTER` | `local[*]` | `spark://spark-master:7077` |

By default the Spark job runs in local mode, which is the normal way to develop Structured Streaming. In this mode the `spark-master` and `spark-worker` containers stay idle, so the Spark Master UI shows no running applications.

To submit the job to the Spark cluster instead, set `SPARK_MASTER`, `CASSANDRA_HOST` and `KAFKA_BOOTSTRAP` to their Docker values and run it through `spark-submit` inside the `spark-master` container. The job then appears in the Spark Master UI.

```bash
docker exec -e HOME=/tmp \
  -e SPARK_MASTER=spark://spark-master:7077 \
  -e CASSANDRA_HOST=cassandra \
  -e KAFKA_BOOTSTRAP=broker:29092 \
  spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 \
  /opt/spark-apps/spark_stream.py
```