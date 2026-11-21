package com.skimsearch.crawl;

public final class CrawlingRole {

    private final GutenbergClient client;
    private final DatalakeWriter writer;

    public CrawlingRole(
            GutenbergClient client,
            DatalakeWriter writer
    ) {
        this.client = client;
        this.writer = writer;
    }

    public Result ingestBook(int bookId)
            throws Exception {

        DatalakeWriter.State current =
                writer.state(bookId);

        if (current == DatalakeWriter.State.READY) {
            return Result.ALREADY_READY;
        }

        GutenbergClient.BookContent content =
                client.download(bookId);

        boolean written =
                writer.write(bookId, content);

        return written
                ? Result.DOWNLOADED
                : Result.ALREADY_READY;
    }

    public enum Result {
        DOWNLOADED,
        ALREADY_READY
    }
}