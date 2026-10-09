# Distributed Task Scheduling System

A fault-tolerant distributed task scheduler built with **Java**, **Apache Kafka**, and **Redis**.

## How it works

```
            ┌───────────┐   tasks topic   ┌───────────┐
            │ Producer  │ ──────────────▶ │  Kafka    │
            │(Scheduler)│                 │  Broker   │
            └───────────┘                 └─────┬─────┘
                  │                           │ consume
                  │ track state               ▼
                  │                     ┌───────────┐
                  └────────────────────▶│  Redis    │◀──── state updates
                                        └───────────┘     (workers)
```

1. **Producer** (`TaskProducer`) — submits tasks as JSON messages to the Kafka `tasks` topic and records them in Redis as `PENDING`.
2. **Workers** (`TaskWorker`) — consume from the topic, execute the task, and update its status in Redis (`RUNNING` → `COMPLETED` / `FAILED`).
3. **State store** (`RedisTaskStore`) — Redis is the source of truth for task status, so progress survives worker restarts.

## Project structure

```
├── pom.xml
└── src/main/java/com/scheduler/
    ├── Task.java            # task model (id, type, payload, status)
    ├── RedisTaskStore.java  # task state in Redis
    ├── TaskProducer.java    # submits tasks to Kafka
    ├── TaskWorker.java      # consumes + executes tasks
    └── SchedulerApp.java    # entry point: produce | worker
```

## Tech stack

- Java 17, Maven
- Apache Kafka (kafka-clients) — reliable task distribution
- Redis (Jedis) — shared task state
- Jackson — JSON serialization

## How to run

Needs Kafka and Redis running locally.

```bash
# 1. Create the topic
kafka-topics.sh --create --topic tasks --bootstrap-server localhost:9092

# 2. Build
mvn package

# 3. Submit tasks (producer)
java -cp target/distributed-task-scheduler-1.0.0.jar:... com.scheduler.SchedulerApp produce

# 4. In another terminal, start a worker
java -cp target/distributed-task-scheduler-1.0.0.jar:... com.scheduler.SchedulerApp worker
```

(Use `mvn dependency:build-classpath` to assemble the runtime classpath, or `mvn exec:java`.)
