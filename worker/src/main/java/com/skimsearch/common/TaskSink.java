package com.skimsearch.common;

import java.util.List;

/**
 * Minimal contract between Ingestion and the Worker.
 * It is at common to avoid circularity dependencies (worker -> ingest -> worker).
 */
public interface TaskSink extends AutoCloseable {
    void bookReady(int bookId);

    void submitIngest(int bookId);

    void submitIndexBatch(List<Integer> bookIds);

    @Override
    default void close() {
    }
}