package com.example.sources;

import com.google.common.collect.ImmutableList;
import lombok.RequiredArgsConstructor;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;

import java.io.IOException;
import java.util.List;

@RequiredArgsConstructor
public class UrlBoundedSource extends BoundedSource<String> {
    private final String sourceUrl;

    @Override
    public List<? extends BoundedSource<String>> split(long desiredBundleSizeBytes, PipelineOptions options) throws Exception {
        return ImmutableList.of(this);
    }

    @Override
    public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
        return 0;
    }

    @Override
    public BoundedReader<String> createReader(PipelineOptions options) throws IOException {
        return new UrlBoundedReader(this, sourceUrl);
    }

    @Override
    public Coder getOutputCoder() {
        return StringUtf8Coder.of();
    }
}
