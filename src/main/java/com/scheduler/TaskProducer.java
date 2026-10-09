package com.scheduler;

import java.util.Properties;
import java.util.UUID;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Submits tasks to the Kafka "tasks" topic and tracks them in Redis. */
public class TaskProducer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(TaskProducer.class);
    private static final String TOPIC = "tasks";

    private final KafkaProducer<String, String> producer;
    private final RedisTaskStore store = new RedisTaskStore();
    private final ObjectMapper mapper = new ObjectMapper();

    public TaskProducer(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        this.producer = new KafkaProducer<>(props);
    }

    /** Create a task, persist it as PENDING, and publish it. Returns the task id. */
    public String submit(String type, String payload) throws Exception {
        String id = UUID.randomUUID().toString();
        Task task = new Task(id, type, payload);
        store.saveTask(task);
        String json = mapper.writeValueAsString(task);
        producer.send(new ProducerRecord<>(TOPIC, id, json)).get();
        log.info("Submitted task {} (type={})", id, type);
        return id;
    }

    @Override
    public void close() {
        producer.close();
        store.close();
    }
}
