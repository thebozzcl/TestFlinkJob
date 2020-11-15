
package com.example;

import java.util.Arrays;

import com.example.sources.UrlBoundedSource;
import com.example.transforms.ToLowerCase;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TypeDescriptors;

public class MinimalWordCount {

    public static void main(final String[] args) {

        final PipelineOptions options = PipelineOptionsFactory.create();
        options.setRunner(FlinkRunner.class);
        final Pipeline p = Pipeline.create(options);

        final PCollectionList<String> inputs = PCollectionList
                .of(p.apply(Read.from(new UrlBoundedSource("http://192.168.1.4/testdata/shakespeare-alls-11.txt"))))
                .and(p.apply(Read.from(new UrlBoundedSource("http://192.168.1.4/testdata/shakespeare-life-56.txt"))))
                .and(p.apply(Read.from(new UrlBoundedSource("http://192.168.1.4/testdata/shakespeare-pericles-21.txt"))))
                .and(p.apply(Read.from(new UrlBoundedSource("http://192.168.1.4/testdatawssnt10.txt"))));

        inputs.apply(Flatten.pCollections())
                .apply(
                        FlatMapElements.into(TypeDescriptors.strings())
                                .via((String line) -> Arrays.asList(line.split("[^\\p{L}]+"))))
                .apply(Filter.by((String word) -> !word.isEmpty()))
                .apply(ParDo.of(new ToLowerCase()))
                .apply(Count.perElement())
                .apply(
                        MapElements.into(TypeDescriptors.strings())
                                .via(
                                        (KV<String, Long> wordCount) ->
                                                wordCount.getKey() + ": " + wordCount.getValue()))
                .apply(TextIO.write().to("wordcounts.txt"));

        p.run().waitUntilFinish();
    }
}
