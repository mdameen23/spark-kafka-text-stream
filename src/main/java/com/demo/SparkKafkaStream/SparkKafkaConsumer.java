package com.demo.SparkKafkaStream;

import java.io.Serializable;
import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import consumer.kafka.MessageAndMetadata;
import consumer.kafka.ReceiverLauncher;

import org.apache.log4j.Logger;
import org.apache.log4j.LogManager;

public class SparkKafkaConsumer implements Serializable
{
    private static final long serialVersionUID = 4332618245650072140L;
    private static Logger logger = LogManager.getLogger(SparkKafkaConsumer.class.getName());
    private Properties props;

    public SparkKafkaConsumer(Properties pr) {
        props = pr;
    }

    public void start() throws InstantiationException, IllegalAccessException,
        ClassNotFoundException  {
        run();
    }

    private void run()  {
        SparkConf _sparkConf = new SparkConf().setAppName("SparkKafkaReceiver").set(
                "spark.streaming.receiver.writeAheadLog.enable", "false");
        _sparkConf.setMaster("local[4]");

        JavaStreamingContext jsc = new JavaStreamingContext(_sparkConf, new Duration(1000));
        int numberOfReceivers = 1;

        JavaDStream<MessageAndMetadata> unionStreams = ReceiverLauncher.launch(
                jsc, props, numberOfReceivers, StorageLevel.MEMORY_ONLY());

        logger.debug("Process RDD");
        unionStreams.foreachRDD(new ReadMessage());

        jsc.start();
        jsc.awaitTermination();
    }
}
