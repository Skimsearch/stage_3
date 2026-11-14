package com.skimsearch.common;

public record SearchResult(
        int bookId,
        String title,
        String author,
        String language,
        Integer year,
        int tf
) {
}