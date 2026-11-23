package com.skimsearch.crawl;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public final class GutenbergClient implements BookDownloader {

    private static final Pattern START_MARKER =
            Pattern.compile(
                    "\\*\\*\\* START OF (?:THE|THIS) PROJECT GUTENBERG EBOOK"
            );

    private static final Pattern END_MARKER =
            Pattern.compile(
                    "\\*\\*\\* END OF (?:THE|THIS) PROJECT GUTENBERG EBOOK"
            );

    private final HttpClient client;

    public GutenbergClient() {
        this.client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .followRedirects(HttpClient.Redirect.NORMAL)
                .build();
    }

    @Override
    public BookContent download(int bookId)
            throws IOException, InterruptedException {

        if (bookId <= 0) {
            throw new IllegalArgumentException(
                    "bookId must be greater than zero"
            );
        }

        URI uri = URI.create(
                "https://www.gutenberg.org/cache/epub/"
                        + bookId
                        + "/pg"
                        + bookId
                        + ".txt"
        );

        int maxAttempts = 3;

        for (int attempt = 1; attempt <= maxAttempts; attempt++) {

            HttpRequest request = HttpRequest.newBuilder(uri)
                    .timeout(Duration.ofSeconds(30))
                    .GET()
                    .build();

            HttpResponse<String> response =
                    client.send(
                            request,
                            HttpResponse.BodyHandlers.ofString()
                    );

            int status = response.statusCode();

            if (status == 200) {
                return split(response.body());
            }

            if (status == 429 || status == 503) {

                if (attempt == maxAttempts) {
                    throw new IOException(
                            "Gutenberg returned HTTP "
                                    + status
                                    + " after "
                                    + maxAttempts
                                    + " attempts for book "
                                    + bookId
                    );
                }

                long waitSeconds =
                        retryAfterSeconds(response, attempt);

                Thread.sleep(waitSeconds * 1000L);
                continue;
            }

            throw new IOException(
                    "Gutenberg returned HTTP "
                            + status
                            + " for book "
                            + bookId
            );
        }

        throw new IOException(
                "Unexpected Gutenberg download failure for book "
                        + bookId
        );
    }

    private static long retryAfterSeconds(
            HttpResponse<?> response,
            int attempt
    ) {
        String retryAfter =
                response.headers()
                        .firstValue("Retry-After")
                        .orElse(null);

        if (retryAfter != null) {
            try {
                long seconds = Long.parseLong(retryAfter);

                /*
                 * Avoid sleeping for many hours inside one worker task.
                 * Long server-side Retry-After values are capped so that
                 * the distributed task system can fail and retry later.
                 */
                return Math.min(seconds, 30);

            } catch (NumberFormatException ignored) {
                // Fall back to a short incremental delay.
            }
        }

        return Math.min(5L * attempt, 15L);
    }

    static BookContent split(String text) {

        Matcher startMatcher = START_MARKER.matcher(text);
        Matcher endMatcher = END_MARKER.matcher(text);

        if (!startMatcher.find() || !endMatcher.find()) {
            throw new IllegalArgumentException(
                    "Project Gutenberg markers not found"
            );
        }

        int start = startMatcher.start();
        int end = endMatcher.start();

        if (end <= start) {
            throw new IllegalArgumentException(
                    "Invalid Gutenberg marker order"
            );
        }

        int bodyStart = text.indexOf('\n', startMatcher.end());

        if (bodyStart < 0 || bodyStart >= end) {
            throw new IllegalArgumentException(
                    "Invalid Gutenberg start marker"
            );
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