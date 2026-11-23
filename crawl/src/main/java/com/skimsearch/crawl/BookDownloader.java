package com.skimsearch.crawl;

public interface BookDownloader {

    GutenbergClient.BookContent download(int bookId) throws Exception;
}