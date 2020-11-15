package com.example.sources;

import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.BoundedSource.BoundedReader;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.NoSuchElementException;

public class UrlBoundedReader extends BoundedReader<String> {
    private final URL sourceUrl;
    private final BoundedSource<String> source;

    private BufferedReader reader;
    private String currentLine = null;

    public UrlBoundedReader(final BoundedSource<String> source, final String sourceUrlString) throws MalformedURLException {
        this.sourceUrl = new URL(sourceUrlString);
        this.source = source;
    }

    @Override
    public boolean start() throws IOException {
        try {
            reader = new BufferedReader(new InputStreamReader(sourceUrl.openStream()));
            currentLine = null;
        } catch (final Throwable t) {
            throw new IOException(t);
        }

        return advance();
    }

    @Override
    public boolean advance() throws IOException {
        if (reader == null) {
            throw new IOException();
        }
        if (!reader.ready()) {
            return false;
        }

        final String input = reader.readLine();
        if (input == null) {
            return false;
        }
        currentLine = input;
        return true;
    }

    @Override
    public String getCurrent() throws NoSuchElementException {
        if (currentLine != null) {
            return currentLine;
        }
        throw new NoSuchElementException();
    }

    @Override
    public void close() throws IOException {
        try {
            if (reader != null) {
                reader.close();
            }
            reader = null;
        } catch (final Throwable t) {
            throw new IOException(t);
        }
    }

    @Override
    public BoundedSource<String> getCurrentSource() {
        return source;
    }
}
