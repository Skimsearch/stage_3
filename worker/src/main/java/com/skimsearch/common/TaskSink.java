package com.skimsearch.common;

import java.util.List;

/**
 * Contrato mínimo entre Ingestion y Worker.
 * Vive en common para evitar dependencias circulares (worker -> ingest -> worker).
 */
public interface TaskSink extends AutoCloseable {
    void bookReady(int bookId);

    void submitIngest(int bookId);

    void submitIndexBatch(List<Integer> bookIds);

    @Override
    default void close() {
    }
}