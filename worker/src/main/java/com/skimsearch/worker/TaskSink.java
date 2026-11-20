package com.skimsearch.worker;

import java.util.List;

/**
 * Contrato mínimo para que el rol de Ingestion encole trabajo
 * sin depender directamente del módulo worker (evita ciclos de dependencias).
 */
public interface TaskSink extends AutoCloseable {
    void bookReady(int bookId);

    void submitIngest(int bookId);

    void submitIndexBatch(List<Integer> bookIds);

    @Override
    default void close() {
    }
}