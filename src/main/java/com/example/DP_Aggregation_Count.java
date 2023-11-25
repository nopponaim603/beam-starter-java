package com.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.transforms.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.beam.sdk.values.KV;

/// Sample form https://tour.beam.apache.org/tour/java/common-transforms/aggregations/count

public class DP_Aggregation_Count {

    private static final Logger LOG = LoggerFactory.getLogger(DP_Aggregation_Count.class);

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);

        // Create input PCollection
        PCollection<Integer> input =
                pipeline.apply(Create.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));

        // The applyTransform() converts [input] to [output]
        PCollection<Long> output = applyTransform(input);

        output.apply("Log", ParDo.of(new LogOutput<>("Input has elements")));

        pipeline.run();
    }

    // Count.globally() to return the globally count from `PCollection`
    static PCollection<Long> applyTransform(PCollection<Integer> input) {
        return input.apply(Count.globally());
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
