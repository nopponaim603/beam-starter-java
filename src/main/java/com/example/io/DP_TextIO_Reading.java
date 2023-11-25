package com.example.io;


import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

public class DP_TextIO_Reading {

    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create(PipelineOptionsFactory.create());

        pipeline.apply(TextIO.read().from("myfile.txt"))
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
}
