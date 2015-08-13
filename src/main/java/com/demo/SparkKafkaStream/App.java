package com.demo.SparkKafkaStream;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.log4j.LogManager;


public class App
{
	private static Logger logger = LogManager.getLogger(App.class.getName());
	
    public static void main( String[] args ) throws Exception
    {
        Properties props = new Properties();
        try {
            logger.debug("Loading Properties for Kafka");
            props.load(new FileInputStream("src/main/resources/consumer.properties"));
        } catch(IOException ex)
        {
            System.out.println(ex.toString());
            return;
        }

        SparkKafkaConsumer consumer = new SparkKafkaConsumer(props);
        consumer.start();
    }
}
