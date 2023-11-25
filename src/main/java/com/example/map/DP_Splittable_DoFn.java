package com.example.map;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/// Sample form https://tour.beam.apache.org/tour/java/splittable-dofn/splittable

public class DP_Splittable_DoFn {
    public static void main(String[] args) {

        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();

        Pipeline pipeline = Pipeline.create(options);

        PCollection<String> input = pipeline.apply(Create.of("Lorem Ipsum is simply dummy text of the printing and typesetting industry. Lorem Ipsum has been the industry's standard dummy text ever since the 1500s, when an unknown printer took a galley of type and scrambled it to make a type specimen book. It has survived not only five centuries, but also the leap into electronic typesetting, remaining essentially unchanged. It was popularised in the 1960s with the release of Letraset sheets containing Lorem Ipsum passages, and more recently with desktop publishing software like Aldus PageMaker including versions of Lorem Ipsum."));


        input
                .apply(ParDo.of(new SplitLinesFn()))
                .apply(ParDo.of(new LogOutput<>()));


        pipeline.run().waitUntilFinish();
    }

    static class SplitLinesFn extends DoFn<String, KV<Long, String>> {
        private static final Integer batchSize = 5;

        @ProcessElement
        public void process(ProcessContext c, RestrictionTracker<OffsetRange, Long> tracker) {
            String[] words = c.element().split(" ");
            for (long i = tracker.currentRestriction().getFrom(); tracker.tryClaim(i); ++i) {
                c.output(KV.of(i, words[(int) i]));
            }
        }

        @GetInitialRestriction
        public OffsetRange getInitialRestriction(@Element String e) {
            return new OffsetRange(0, e.split(" ").length);
        }

        @GetRestrictionCoder
        public Coder<OffsetRange> getRestrictionCoder() {
            return OffsetRange.Coder.of();
        }

        @SplitRestriction
        public void splitRestriction(@Element String input, @Restriction OffsetRange restriction, OutputReceiver<OffsetRange> receiver) throws Exception {
            long start = restriction.getFrom();
            long size = restriction.getTo();
            long splitSizeBytes = size / batchSize;

            while (start < size) {
                long splitEnd = start + splitSizeBytes;
                if (splitEnd >= size) {
                    splitEnd = size;
                }
                receiver.output(new OffsetRange(start, splitEnd));
                start = splitEnd;
            }
        }
    }

    static class LogOutput<T> extends DoFn<T, T> {

        private static final Logger LOG = LoggerFactory.getLogger(DP_Splittable_DoFn.class);

        private final String prefix;

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
