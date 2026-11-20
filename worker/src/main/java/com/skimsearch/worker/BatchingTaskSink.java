package com.skimsearch.worker;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.skimsearch.common.TaskSink;

/**
 * Implementación de TaskSink que agrupa libros listos en lotes de tamaño N
 * o cada T milisegundos para evitar crear una versión completa de SQLite por cada libro.
 */
public final class BatchingTaskSink implements TaskSink {

    private final DistributedTaskQueue queue;
    private final String workerId;
    private final int batchSize;
    private final int maxAttempts;
    private final List<Integer> buffer = new ArrayList<>();
    private final AtomicInteger batchSequence = new AtomicInteger(0);
    private final ScheduledExecutorService flusher;

    public BatchingTaskSink(DistributedTaskQueue queue,
                            String workerId,
                            int batchSize,
                            long flushIntervalMs,
                            int maxAttempts) {
        this.queue = queue;
        this.workerId = workerId;
        this.batchSize = Math.max(1, batchSize);
        this.maxAttempts = maxAttempts;
        this.flusher = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "index-batch-flusher");
            t.setDaemon(true);
            return t;
        });
        this.flusher.scheduleAtFixedRate(this::flush, flushIntervalMs, flushIntervalMs, TimeUnit.MILLISECONDS);
    }

    @Override
    public synchronized void bookReady(int bookId) {
        buffer.add(bookId);
        if (buffer.size() >= batchSize) {
            flush();
        }
    }

    @Override
    public void submitIngest(int bookId) {
        Task task = new Task("ingest-" + bookId, "ingest", List.of(bookId), 0, maxAttempts);
        queue.submit(task);
    }

    @Override
    public void submitIndexBatch(List<Integer> bookIds) {
        if (bookIds == null || bookIds.isEmpty()) {
            return;
        }
        int seq = batchSequence.incrementAndGet();
        String taskId = "index-" + workerId + "-" + System.currentTimeMillis() + "-" + seq;
        Task task = new Task(taskId, "index", bookIds, 0, maxAttempts);
        queue.submit(task);
    }

    public synchronized void flush() {
        if (buffer.isEmpty()) {
            return;
        }
        List<Integer> batch = new ArrayList<>(buffer);
        buffer.clear();
        submitIndexBatch(batch);
    }

    @Override
    public void close() {
        flusher.shutdown();
        flush();
    }
}