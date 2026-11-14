package com.skimsearch.common;

/**
 * The index cannot answer right now (missing, being replaced, unreachable).
 * Search turns this into HTTP 503, never into an empty result list (contract rule 6).
 */
public class IndexUnavailableException extends RuntimeException {

    public IndexUnavailableException(String message) {
        super(message);
    }

    public IndexUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}