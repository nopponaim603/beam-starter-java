package com.gameanalytics;

import static org.apache.beam.sdk.values.TypeDescriptors.integers;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DP_Read_and_split {

    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create(PipelineOptionsFactory.create());

        pipeline.apply(TextIO.read().from("sample.json"))
                .apply(ParDo.of(new DoFn<String, Void>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        String[] line = c.element().split(" ");
                        for (String word : line) {
                            System.out.println(word);
                        }
                    }
                }));

        pipeline.run().waitUntilFinish();
    }

    /*

    static  PCollection<String, Void> processElementIO(PCollection<String, Void> input) {
        return processElementIO(null);
    }
*/


    // The applyTransform() multiplies the number by 10 and outputs
    static PCollection<Integer> applyTransform(PCollection<Integer> input) {
        return input.apply(ParDo.of(new DoFn<Integer, Integer>() {

            @ProcessElement
            public void processElement(@Element Integer number, OutputReceiver<Integer> out) {
                out.output(number * 10);
            }

        }));
    }
}
