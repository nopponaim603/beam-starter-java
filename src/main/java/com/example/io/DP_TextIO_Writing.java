package com.example.io;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;

public class DP_TextIO_Writing {
    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create(PipelineOptionsFactory.create());

        pipeline.apply(Create.of("Hello, World!"))
                .apply(TextIO.write().to("myfile.txt"));

        pipeline.run();
    }
}
