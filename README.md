# Event Platform

## Description

Event Platform is a backend system for ingesting, processing, and analyzing financial transaction events at scale.

The project is built around an event-driven architecture. Incoming events are accepted through a REST API and immediately published to Apache Kafka. A separate consumer service processes those events and stores them in PostgreSQL. Analytics endpoints are accelerated with Redis caching, and the long-term deployment target is AWS EKS.

I built this project to gain hands-on experience with distributed systems concepts such as asynchronous processing, message queues, caching, observability, Kubernetes, and production-style deployments.


## Architecture

![The Architecture of Event Platform](/docs/architecture.png "Event Platform Architecture")

When a client submits an event, the API validates the request and publishes it to Kafka. The request is acknowledged immediately without waiting for a database write. A separate consumer service reads events from Kafka and persists them to PostgreSQL.

This approach provides several advantages:

* API latency remains low even during database spikes.
* Additional consumers can be added independently.
* Kafka acts as a durable buffer during traffic bursts.
* Producers and consumers can evolve separately.


## Project Setup

### Prerequisites

- Docker 24+ or Docker Desktop
- Go 1.22+ 
- Confluent Cloud account
- Git

### Environment Variables

Create a `.env` file in the project root.

```
KAFKA_BOOTSTRAP_SERVERS=pkc-xxxxx.us-east1.aws.confluent.cloud:9092
KAFKA_API_KEY=your_api_key
KAFKA_API_SECRET=your_api_secret
DB_URL=postgres://event-platform:password@localhost:5432/event-platform
REDIS_URL=localhost:6379
```

A template is available in `.env.example` in the repo root

### Installing Docker (Ubuntu / WSL2)

```bash
sudo apt install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
sudo systemctl status docker
```

### Installing Go

Download the latest Go release from [https://go.dev/doc/install](https://go.dev/doc/install) and follow the installation instructions for your platform. 

### Confluent Cloud Setup

1. Create a free Confluent Cloud Account
2. Create a Basic cluster in AWS us-east-1
3. Create a topic named `events` with 3 partitions
4. Generate an API Key and add credentials to `.env` file

### Starting the Docker Containers

PostgreSQL and Redis run as Docker containers. From the project root:

```bash
docker compose up -d
docker compose ps
```

Wait until both containers report a healthy status.

### Database Setup

Run the schema file to create the events table and indexes. This must be done once before starting the consumer:

```bash
docker exec -i event-platform-db-1 psql -U event-platform -d event-platform < db/schema.sql
```

Verify the table was created:

```bash
docker exec -it event-platform-db-1 psql -U event-platform -d event-platform -c "\d events"
```

## Running the Project

**Terminal 1 — start the API server (producer):**

```bash
go run cmd/api/main.go
```

**Terminal 2 — start the consumer:**

```bash
go run cmd/consumer/main.go
```

Once both are running, send events via curl:

**POST /events** — ingest a new financial transaction event:

```bash
curl -X POST http://localhost:8080/events \
  -H "Content-Type: application/json" \
  -d '{"type":"payment","payload":{"amount":100,"currency":"USD"},"timestamp":"2026-06-03T10:00:00Z"}'
```

Expected response:
```json
{"uuid":"bc761dca-9567-4e37-aba1-352660572154"}
```

**GET /events/summary** — retrieve aggregated analytics across all events:

```bash
curl http://localhost:8080/events/summary
```

Expected response:
```json
{
  "total_events": 4,
  "by_type": {
    "payment": 1,
    "transfer": 1,
    "withdrawal": 1,
    "invest": 1
  },
  "latest_event": "2026-06-03T11:45:00Z"
}
```

## Tech Stack

### Go

I chose Go for both services because of its strong concurrency model, low memory footprint, and excellent performance for backend systems.

### Apache Kafka

Kafka serves as the backbone of the platform. It decouples ingestion from processing, provides buffering during traffic spikes, and allows multiple consumers to process the same stream independently.

### PostgreSQL

PostgreSQL stores processed events and analytics data. JSONB support makes it easy to handle flexible event payloads while retaining strong transactional guarantees.

### Redis

Redis is used to cache frequently requested analytics responses and reduce database load.

### Docker

Docker provides a consistent local development environment and simplifies deployment workflows.

### AWS EKS

The production deployment target is Kubernetes on AWS EKS, allowing services to scale independently based on workload.