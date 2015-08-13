package com.demo.SparkKafkaStream;

import java.util.List;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Time;

import consumer.kafka.MessageAndMetadata;

public class ReadMessage implements Function2<JavaRDD<MessageAndMetadata>, Time, Void> {
	
	private static Logger logger = LogManager.getLogger(ReadMessage.class.getName());
	private static final long serialVersionUID = 4232618245650072140L;
	
	public Void call(JavaRDD<MessageAndMetadata> rdd, Time time) throws Exception {

        List<MessageAndMetadata> vals = rdd.collect();
        logger.info("Number of records in this batch " + vals.size());

        if (vals.size() > 0) {
            for (MessageAndMetadata md:vals) {
                byte bytes[] = md.getPayload();
                String st = new String(bytes);
                logger.info("Message: " + st);
            }
        }

        return null;
    }
}

