package com.scheduler;

import com.fasterxml.jackson.databind.ObjectMapper;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

/**
 * Redis-backed task state store. Every task's lifecycle
 * (PENDING -> RUNNING -> COMPLETED/FAILED) is recorded here,
 * so state survives worker restarts.
 */
public class RedisTaskStore implements AutoCloseable {

    private static final String KEY_PREFIX = "task:";
    private final JedisPool pool = new JedisPool("localhost", 6379);
    private final ObjectMapper mapper = new ObjectMapper();

    public void saveTask(Task task) {
        try (Jedis jedis = pool.getResource()) {
            jedis.set(KEY_PREFIX + task.getId(), mapper.writeValueAsString(task));
        } catch (Exception e) {
            throw new RuntimeException("Failed to save task " + task.getId(), e);
        }
    }

    public void updateStatus(String taskId, Task.Status status) {
        try (Jedis jedis = pool.getResource()) {
            String json = jedis.get(KEY_PREFIX + taskId);
            if (json == null) {
                return;
            }
            Task task = mapper.readValue(json, Task.class);
            task.setStatus(status);
            jedis.set(KEY_PREFIX + taskId, mapper.writeValueAsString(task));
        } catch (Exception e) {
            throw new RuntimeException("Failed to update task " + taskId, e);
        }
    }

    public Task getTask(String taskId) {
        try (Jedis jedis = pool.getResource()) {
            String json = jedis.get(KEY_PREFIX + taskId);
            return json == null ? null : mapper.readValue(json, Task.class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to read task " + taskId, e);
        }
    }

    @Override
    public void close() {
        pool.close();
    }
}
