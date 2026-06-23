FROM golang:1.26-bookworm AS builder

RUN apt-get update && apt-get install -y librdkafka-dev gcc

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=1 GOOS=linux go build -o /app/bin/fraud-consumer ./cmd/fraud-consumer

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y librdkafka1 ca-certificates && rm -rf /var/lib/apt/lists/*
 
WORKDIR /app

COPY --from=builder /app/bin/fraud-consumer .

CMD ["./fraud-consumer"]