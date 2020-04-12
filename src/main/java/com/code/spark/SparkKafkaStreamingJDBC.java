package com.code.spark;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class SparkKafkaStreamingJDBC {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("Spark Kafka Demo");
        JavaStreamingContext streamingContext = new JavaStreamingContext(conf, Duration.apply(3));
        Map<String, Object> kafkaParams = new HashMap<String, Object>();

        // for local setup
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer",
                StringDeserializer.class);
        kafkaParams.put("value.deserializer",
                StringDeserializer.class);
        kafkaParams.put("group.id",
                "use_a_separate_group_id_for_each_stream");
        // When you want to start , earliest or latest
        kafkaParams.put("auto.offset.reset",
                "earliest");
        kafkaParams.put("enable.auto.commit", false);

        Collection<String> topics = Arrays.asList("jdbc-source-jdbc_source");

        final JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(streamingContext
                , LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));

        JavaDStream<String> reval = stream.map(new Function<ConsumerRecord<String, String>, String>() {

            private static final long serialVersionUID = 1L;

            public String call(ConsumerRecord<String, String> record) {
                return record.value();
            }

        });

        reval.print();
        streamingContext.start();

        try {
            streamingContext.awaitTermination();
        } catch (Exception e) {
            e.printStackTrace();
        }

        while (true) {
            try {
                Thread.sleep(1000);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
