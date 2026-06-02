Real Time Event Processing Platform
============

A high-throughput backend service that ingests events via REST API, routes through Kafka, processes with a Go consumer, stores in PostgreSQL, and serves analytics; deployed on AWS EKS with CI/CD and a Python FastAPI microservice for the LLM integration layer.


Stack:

  * Go
  * Python
  * FastAPI
  * PostgreSQL
  * Kafka