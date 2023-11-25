package com.example.map;

import static org.apache.beam.sdk.values.TypeDescriptors.integers;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/// Sample form https://tour.beam.apache.org/tour/java/core-transforms/branching
public class DP_Branching {

    private static final Logger LOG = LoggerFactory.getLogger(DP_Branching.class);

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);

        // List of elements
        PCollection<Integer> input =
                pipeline.apply(Create.of(1, 2, 3, 4, 5));

        // The applyMultiply5Transform() converts [input] to [mult5Results]
        PCollection<Integer> mult5Results = applyMultiply5Transform(input);

        // The applyMultiply10Transform() converts [input] to [mult10Results]
        PCollection<Integer> mult10Results = applyMultiply10Transform(input);

        mult5Results.apply("Log multiplied by 5: ", ParDo.of(new LogOutput<Integer>("Multiplied by 5: ")));
        mult10Results.apply("Log multiplied by 10: ", ParDo.of(new LogOutput<Integer>("Multiplied by 10: ")));

        pipeline.run();
    }


    // The applyMultiply5Transform return PCollection with elements multiplied by 5
    static PCollection<Integer> applyMultiply5Transform(PCollection<Integer> input) {
        return input.apply("Multiply by 5", MapElements.into(integers()).via(num -> num * 5));
    }

    // The applyMultiply5Transform return PCollection with elements multiplied by 10
    static PCollection<Integer> applyMultiply10Transform(PCollection<Integer> input) {
        return input.apply("Multiply by 10", MapElements.into(integers()).via(num -> num * 10));
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
