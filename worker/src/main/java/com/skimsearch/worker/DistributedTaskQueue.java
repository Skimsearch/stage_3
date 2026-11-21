package com.skimsearch.worker;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.gson.Gson;
import com.hazelcast.collection.IQueue;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;

public final class DistributedTaskQueue {

    public static final String QUEUE_INGEST = "tasks-pending-ingest";
    public static final String QUEUE_INDEX = "tasks-pending-index";
    public static final String MAP_DATA = "tasks-data";
    public static final String MAP_INFLIGHT = "tasks-inflight";
    public static final String MAP_LEASES = "task-leases";
    public static final String MAP_DONE = "tasks-done";
    public static final String MAP_FAILED = "tasks-failed";

    private final IQueue<String> ingestQueue;
    private final IQueue<String> indexQueue;
    private final IMap<String, String> tasksData;
    private final IMap<String, String> inflight;
    private final IMap<String, String> leases;
    private final IMap<String, String> done;
    private final IMap<String, String> failed;

    private final long leaseTtlMs;
    private final WorkerLogger logger;
    private final Gson gson = new Gson();

    public DistributedTaskQueue(HazelcastInstance hz, long leaseTtlMs, WorkerLogger logger) {
        this.ingestQueue = hz.getQueue(QUEUE_INGEST);
        this.indexQueue = hz.getQueue(QUEUE_INDEX);
        this.tasksData = hz.getMap(MAP_DATA);
        this.inflight = hz.getMap(MAP_INFLIGHT);
        this.leases = hz.getMap(MAP_LEASES);
        this.done = hz.getMap(MAP_DONE);
        this.failed = hz.getMap(MAP_FAILED);
        this.leaseTtlMs = leaseTtlMs;
        this.logger = logger;
    }

    /**
     * Envío idempotente: si el ID de tarea ya existía, no se duplica en la cola.
     */
    public boolean submit(Task task) {
        String json = gson.toJson(task);
        if (tasksData.putIfAbsent(task.getId(), json) != null) {
            return false;
        }
        queueForType(task.getType()).offer(task.getId());
        logger.log("task-enqueued",
                "taskId", task.getId(),
                "type", task.getType(),
                "books", task.getBookIds().size(),
                "maxAttempts", task.getMaxAttempts());
        return true;
    }

    /**
     * Reclamación atómica: poll -> lease con TTL -> inflight.
     */
    public Optional<Task> claimNext(String workerId, Set<String> roles, long pollTimeoutMs) throws InterruptedException {
        IQueue<String> targetQueue = selectQueueForRoles(roles);
        if (targetQueue == null) {
            return Optional.empty();
        }

        String taskId = targetQueue.poll(pollTimeoutMs, TimeUnit.MILLISECONDS);
        if (taskId == null) {
            return Optional.empty();
        }

        String json = tasksData.get(taskId);
        if (json == null) {
            return Optional.empty();
        }

        // Orden estricto: primero el lease con TTL y después inflight
        leases.put(taskId, workerId, leaseTtlMs, TimeUnit.MILLISECONDS);
        inflight.put(taskId, json);

        Task task = gson.fromJson(json, Task.class);
        logger.log("task-claimed",
                "taskId", task.getId(),
                "type", task.getType(),
                "attempt", task.getAttempts() + 1,
                "maxAttempts", task.getMaxAttempts());
        return Optional.of(task);
    }

    /**
     * Renueva el latido mientras la tarea sigue en ejecución y pertenece a este worker.
     */
    public boolean renewLease(String taskId, String workerId) {
        String owner = leases.get(taskId);
        if (workerId.equals(owner) && inflight.containsKey(taskId)) {
            leases.put(taskId, workerId, leaseTtlMs, TimeUnit.MILLISECONDS);
            return true;
        }
        return false;
    }

    /**
     * Marca la tarea como completada y registra datos para la matriz trabajador x tarea.
     */
    public void complete(Task task, String workerId, long durationMs) {
        inflight.remove(task.getId());
        leases.remove(task.getId());

        Map<String, Object> record = new LinkedHashMap<>();
        record.put("taskId", task.getId());
        record.put("type", task.getType());
        record.put("workerId", workerId);
        record.put("attempts", task.getAttempts() + 1);
        record.put("durationMs", durationMs);
        record.put("finishedAt", Instant.now().toString());
        record.put("status", "DONE");

        done.put(task.getId(), gson.toJson(record));
        logger.log("task-completed",
                "taskId", task.getId(),
                "type", task.getType(),
                "durationMs", durationMs);
    }

    /**
     * Gestiona el fallo durante la ejecución: reencola o manda a fallidas.
     */
    public void failOrRequeue(Task task, String workerId, String reason) {
        inflight.remove(task.getId());
        leases.remove(task.getId());
        requeueOrFailInternal(task, workerId, reason);
    }

    /**
     * Reaper: detecta tareas en inflight cuyo lease ha caducado y solo un worker gana la recuperación.
     */
    public void reapExpiredLeases(String reaperWorkerId) {
        for (String taskId : inflight.keySet()) {
            if (!leases.containsKey(taskId)) {
                String rawJson = inflight.remove(taskId);
                if (rawJson != null) {
                    Task task = gson.fromJson(rawJson, Task.class);
                    requeueOrFailInternal(task, reaperWorkerId, "lease-expired");
                }
            }
        }
    }

    private void requeueOrFailInternal(Task task, String actorWorkerId, String reason) {
        int nextAttempts = task.getAttempts() + 1;
        task.setAttempts(nextAttempts);
        String updatedJson = gson.toJson(task);
        tasksData.put(task.getId(), updatedJson);

        if (nextAttempts >= task.getMaxAttempts()) {
            Map<String, Object> record = new LinkedHashMap<>();
            record.put("taskId", task.getId());
            record.put("type", task.getType());
            record.put("lastActor", actorWorkerId);
            record.put("attempts", nextAttempts);
            record.put("reason", reason);
            record.put("failedAt", Instant.now().toString());
            record.put("status", "FAILED");

            failed.put(task.getId(), gson.toJson(record));
            logger.log("task-failed",
                    "taskId", task.getId(),
                    "type", task.getType(),
                    "attempts", nextAttempts,
                    "reason", reason);
        } else {
            queueForType(task.getType()).offer(task.getId());
            logger.log("task-requeued",
                    "taskId", task.getId(),
                    "type", task.getType(),
                    "attempt", nextAttempts,
                    "reason", reason);
        }
    }

    private IQueue<String> queueForType(String type) {
        return "ingest".equals(type) ? ingestQueue : indexQueue;
    }

    private IQueue<String> selectQueueForRoles(Set<String> roles) {
        boolean canCrawl = roles.contains("crawl");
        boolean canIndex = roles.contains("index");

        if (canCrawl && canIndex) {
            return !ingestQueue.isEmpty() ? ingestQueue : indexQueue;
        }
        if (canCrawl) {
            return ingestQueue;
        }
        if (canIndex) {
            return indexQueue;
        }
        return null;
    }

    public Map<String, String> snapshotDone() {
        return new LinkedHashMap<>(done);
    }

    public Map<String, String> snapshotFailed() {
        return new LinkedHashMap<>(failed);
    }
}