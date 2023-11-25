package com.example.window;

import com.example.Event;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/// Sample form https://tour.beam.apache.org/tour/java/windowing/adding-timestamp

public class DP_Windowsing {

    private static final Logger LOG = LoggerFactory.getLogger(DP_Windowsing.class);


    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);

        PCollection<Event> input =
                pipeline.apply(
                        Create.of(
                                new Event("1", "book-order", DateTime.parse("2019-06-01T00:00:00+00:00")),
                                new Event("2", "pencil-order", DateTime.parse("2019-06-02T00:00:00+00:00")),
                                new Event("3", "paper-order", DateTime.parse("2019-06-03T00:00:00+00:00")),
                                new Event("4", "pencil-order", DateTime.parse("2019-06-04T00:00:00+00:00")),
                                new Event("5", "book-order", DateTime.parse("2019-06-05T00:00:00+00:00"))
                        )
                );

        PCollection<Event> output = applyTransform(input);

        output.apply("Log", ParDo.of(new LogOutput()));

        pipeline.run();
    }


    static PCollection<Event> applyTransform(PCollection<Event> events) {
        return events.apply(ParDo.of(new DoFn<Event, Event>() {

            @ProcessElement
            public void processElement(@Element Event event, OutputReceiver<Event> out) {
                out.outputWithTimestamp(event, event.getDate().toInstant());
            }

        }));
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


