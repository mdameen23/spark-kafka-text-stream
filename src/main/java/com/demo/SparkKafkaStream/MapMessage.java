package com.demo.SparkKafkaStream;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.Function;
import consumer.kafka.MessageAndMetadata;

public class MapMessage implements Function<MessageAndMetadata, String>{

    private static final long serialVersionUID = 4262618245650072140L;
    private static Logger logger = LogManager.getLogger(MapMessage.class.getName());

    public String call(MessageAndMetadata md) throws Exception {
        byte bytes[] = md.getPayload();
        String st = new String(bytes);
        logger.info("Map Message: " + st);
        return st;
    }

}
