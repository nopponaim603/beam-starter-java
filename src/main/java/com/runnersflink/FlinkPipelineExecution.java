package com.runnersflink;

import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Mean;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

public class FlinkPipelineExecution {

    public static void main(String[] args) {
        // Set Flink specific pipeline options
        FlinkPipelineOptions options = PipelineOptionsFactory.as(FlinkPipelineOptions.class);
        options.setRunner(org.apache.beam.runners.flink.FlinkRunner.class);

        // Set Flink master (replace with your Flink master address)
        options.setFlinkMaster("local");

        // Create a Beam pipeline using FlinkPipelineOptions
        Pipeline pipeline = Pipeline.create(options);

        // Define the pipeline operations
        pipeline.apply(TextIO.read().from("input.txt"))
                .apply("Some Transformation", ParDo.of(new DoFn<String, Void>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        String[] line = c.element().split(" ");
                        for (String word : line) {
                            System.out.println(word);
                        }
                    }
                })); // Add your transformations here
                //.apply(TextIO.write().to("output.txt"));

        // Run the pipeline
        pipeline.run().waitUntilFinish();
    }

    static PCollection<Double> applyTransform(PCollection<Integer> input) {
        return input.apply(Mean.globally());
    }

}
