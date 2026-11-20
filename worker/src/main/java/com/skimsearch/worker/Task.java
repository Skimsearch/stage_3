package com.skimsearch.worker;

import java.time.Instant;
import java.util.List;

public final class Task {
    private String id;
    private String type; // "ingest" o "index"
    private List<Integer> bookIds;
    private int attempts;
    private int maxAttempts;
    private String createdAt;

    public Task() {
    }

    public Task(String id, String type, List<Integer> bookIds, int attempts, int maxAttempts) {
        this.id = id;
        this.type = type;
        this.bookIds = List.copyOf(bookIds);
        this.attempts = attempts;
        this.maxAttempts = maxAttempts;
        this.createdAt = Instant.now().toString();
    }

    public String getId() {
        return id;
    }

    public String getType() {
        return type;
    }

    public List<Integer> getBookIds() {
        return bookIds;
    }

    public int getAttempts() {
        return attempts;
    }

    public void setAttempts(int attempts) {
        this.attempts = attempts;
    }

    public int getMaxAttempts() {
        return maxAttempts;
    }

    public String getCreatedAt() {
        return createdAt;
    }
}