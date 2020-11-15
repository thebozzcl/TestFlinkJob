package com.example.transforms;

import org.apache.beam.sdk.transforms.DoFn;

public class ToLowerCase extends DoFn<String, String> {
    @ProcessElement
    public void processElement(final ProcessContext c) {
        c.output(c.element().toLowerCase());
    }
}
