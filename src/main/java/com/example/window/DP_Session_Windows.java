package com.example.window;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Min;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/// Sample form https://tour.beam.apache.org/tour/java/windowing/session-window
public class DP_Session_Windows {

    private static final Logger LOG = LoggerFactory.getLogger(DP_Session_Windows.class);


    public static void main(String[] args) {
        LOG.info("Running Task");

        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);

        PCollection<String> input =
                pipeline.apply(
                        Create.of("To", "be", "or", "not", "to", "be","that", "is", "the", "question")
                );


        PCollection<String> sessionWindowedItems = input.apply(
                Window.<String>into(Sessions.withGapDuration(Duration.standardSeconds(600))));

        sessionWindowedItems.apply("Log words", ParDo.of(new LogStrings()));


        pipeline.run();
    }

    public static class LogStrings extends DoFn<String, String> {

        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            LOG.info("Processing word: {}", c.element());
            c.output(c.element());
        }
    }
}
