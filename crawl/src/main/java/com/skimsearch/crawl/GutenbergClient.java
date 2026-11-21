package com.skimsearch.crawl;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

public final class GutenbergClient implements BookDownloader {

    private static final String START_MARKER =
            "*** START OF THE PROJECT GUTENBERG EBOOK";

    private static final String END_MARKER =
            "*** END OF THE PROJECT GUTENBERG EBOOK";

    private final HttpClient client;

    public GutenbergClient() {
        this.client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build();
    }

    @Override
    public BookContent download(int bookId)
            throws IOException, InterruptedException {

        if (bookId <= 0) {
            throw new IllegalArgumentException(
                    "bookId must be greater than zero");
        }

        URI uri = URI.create(
                "https://www.gutenberg.org/cache/epub/"
                        + bookId
                        + "/pg"
                        + bookId
                        + ".txt"
        );

        HttpRequest request = HttpRequest.newBuilder(uri)
                .timeout(Duration.ofSeconds(30))
                .GET()
                .build();

        HttpResponse<String> response =
                client.send(
                        request,
                        HttpResponse.BodyHandlers.ofString()
                );

        if (response.statusCode() != 200) {
            throw new IOException(
                    "Gutenberg returned HTTP "
                            + response.statusCode()
                            + " for book "
                            + bookId
            );
        }

        return split(response.body());
    }

    static BookContent split(String text) {

        int start = text.indexOf(START_MARKER);
        int end = text.indexOf(END_MARKER);

        if (start < 0 || end < 0 || end <= start) {
            throw new IllegalArgumentException(
                    "Project Gutenberg markers not found");
        }

        int bodyStart = text.indexOf('\n', start);

        if (bodyStart < 0 || bodyStart >= end) {
            throw new IllegalArgumentException(
                    "Invalid Gutenberg start marker");
        }

        String header = text.substring(0, start).trim();
        String body = text.substring(bodyStart + 1, end).trim();

        return new BookContent(header, body);
    }

    public record BookContent(
            String header,
            String body
    ) {
    }
}