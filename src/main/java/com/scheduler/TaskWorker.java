package com.scheduler;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Worker: consumes tasks from Kafka, executes them, and records
 * the outcome in Redis. Run as many workers as you like — Kafka
 * partitions the load across the consumer group.
 */
public class TaskWorker implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(TaskWorker.class);
    private static final String TOPIC = "tasks";

    private final KafkaConsumer<String, String> consumer;
    private final RedisTaskStore store = new RedisTaskStore();
    private final ObjectMapper mapper = new ObjectMapper();
    private volatile boolean running = true;

    public TaskWorker(String bootstrapServers, String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        this.consumer = new KafkaConsumer<>(props);
    }

    public void run() {
        consumer.subscribe(Collections.singletonList(TOPIC));
        log.info("Worker started, listening on topic '{}'", TOPIC);
        while (running) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
            for (ConsumerRecord<String, String> record : records) {
                handle(record.value());
            }
            consumer.commitSync();
        }
    }

    private void handle(String json) {
        try {
            Task task = mapper.readValue(json, Task.class);
            store.updateStatus(task.getId(), Task.Status.RUNNING);
            log.info("Executing task {} (type={})", task.getId(), task.getType());
            execute(task);
            store.updateStatus(task.getId(), Task.Status.COMPLETED);
            log.info("Task {} completed", task.getId());
        } catch (Exception e) {
            log.error("Task failed: {}", e.getMessage());
            try {
                Task task = mapper.readValue(json, Task.class);
                store.updateStatus(task.getId(), Task.Status.FAILED);
            } catch (Exception ignored) {
            }
        }
    }

    /** Task execution logic, dispatched by task type. */
    private void execute(Task task) throws InterruptedException {
        // Simulated work per task type; replace with real handlers.
        switch (task.getType()) {
            case "email":
                Thread.sleep(500);
                break;
            case "report":
                Thread.sleep(1500);
                break;
            default:
                Thread.sleep(300);
        }
    }

    public void shutdown() {
        running = false;
    }

    @Override
    public void close() {
        consumer.close();
        store.close();
    }
}
