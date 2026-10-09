package com.scheduler;

import com.fasterxml.jackson.annotation.JsonProperty;

/** A unit of work flowing through the scheduler. */
public class Task {

    public enum Status {
        PENDING, RUNNING, COMPLETED, FAILED
    }

    @JsonProperty("id")
    private String id;

    @JsonProperty("type")
    private String type;

    @JsonProperty("payload")
    private String payload;

    @JsonProperty("status")
    private Status status;

    public Task() {
    }

    public Task(String id, String type, String payload) {
        this.id = id;
        this.type = type;
        this.payload = payload;
        this.status = Status.PENDING;
    }

    public String getId() {
        return id;
    }

    public String getType() {
        return type;
    }

    public String getPayload() {
        return payload;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }
}
