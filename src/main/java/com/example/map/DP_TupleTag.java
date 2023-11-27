package com.example.map;


import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Arrays;
import org.apache.beam.sdk.values.TypeDescriptors;

/// Sample form https://tour.beam.apache.org/tour/java/core-transforms/additional-outputs

public class DP_TupleTag {

    private static final Logger LOG = LoggerFactory.getLogger(DP_TupleTag.class);


    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);

        // List of elements
        PCollection<Integer> input = pipeline.apply(Create.of(10, 50, 120, 20, 200, 0));

        TupleTag<Integer> numBelow100Tag = new TupleTag<Integer>() {};
        TupleTag<Integer> numAbove100Tag = new TupleTag<Integer>() {};

        // The applyTransform() converts [input] to [outputTuple]
        PCollectionTuple outputTuple = applyTransform(input, numBelow100Tag, numAbove100Tag);

        outputTuple.get(numBelow100Tag).apply("Log Number <= 100: ", ParDo.of(new LogOutput<Integer>("Number <= 100: ")));



        outputTuple.get(numAbove100Tag).apply("Log Number > 100: ", ParDo.of(new LogOutput<Integer>("Number > 100: ")));


        pipeline.run();
    }

    // The function has multiple outputs, numbers above 100 and below
    static PCollectionTuple applyTransform(
            PCollection<Integer> input, TupleTag<Integer> numBelow100Tag,
            TupleTag<Integer> numAbove100Tag) {

        return input.apply(ParDo.of(new DoFn<Integer, Integer>() {

            @ProcessElement
            public void processElement(@Element Integer number, MultiOutputReceiver out) {
                if (number <= 100) {
                    // First PCollection
                    out.get(numBelow100Tag).output(number);
                } else {
                    // Additional PCollection
                    out.get(numAbove100Tag).output(number);
                }
            }

        }).withOutputTags(numBelow100Tag, TupleTagList.of(numAbove100Tag)));
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
