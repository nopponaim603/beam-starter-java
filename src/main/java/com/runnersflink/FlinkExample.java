package com.runnersflink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class FlinkExample {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Create a socket stream source
        DataStream<String> text = env.socketTextStream("localhost", 9999);

        // Process the data
        DataStream<Tuple2<String, Integer>> wordCounts = text
                .flatMap(new Tokenizer())
                .keyBy(0)
                .sum(1);

        wordCounts.print();

        env.execute("Flink Streaming WordCount");
    }

    // Tokenizer function
    public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            // Normalize and split the line
            String[] words = value.toLowerCase().split("\\W+");

            // Emit the words
            for (String word : words) {
                if (word.length() > 0) {
                    out.collect(new Tuple2<>(word, 1));
                }
            }
        }
    }
}
