package com.example.coretransforms;


import static org.apache.beam.sdk.values.TypeDescriptors.integers;

import java.util.Arrays;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DP_Composite {

    private static final Logger LOG = LoggerFactory.getLogger(DP_Composite.class);


    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);

        pipeline
                // List of elements
                .apply(Create.of("1,2,3,4,5", "6,7,8,9,10"))

                // Composite operation
                .apply(new ExtractAndMultiplyNumbers())

                .apply("Log", ParDo.of(new LogOutput<Integer>()));


        pipeline.run();
    }

    // The class with PTransform
    static class ExtractAndMultiplyNumbers
            extends PTransform<PCollection<String>, PCollection<Integer>> {

        // First operation
        @Override
        public PCollection<Integer> expand(PCollection<String> input) {
            return input
                    .apply(ParDo.of(new DoFn<String, Integer>() {
                        // Second operation
                        @ProcessElement
                        public void processElement(@Element String numbers, OutputReceiver<Integer> out) {
                            Arrays.stream(numbers.split(","))
                                    .forEach(numStr -> out.output(Integer.parseInt(numStr)));
                        }

                    }))

                    .apply(MapElements.into(integers()).via(number -> number * 10));
        }

    }

    static class LogOutput<T> extends DoFn<T, T> {
        private String prefix;

        LogOutput() {
            this.prefix = "Processing element";
        }

        LogOutput(String prefix) {
            this.prefix = prefix;
        }

        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            LOG.info(prefix + ": {}", c.element());
        }
    }
}
