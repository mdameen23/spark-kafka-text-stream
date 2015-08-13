package com.demo.SparkKafkaStream;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.Function;

import com.opencsv.CSVReader;

import java.io.StringReader;

public class ParseLine implements Function<String, String[]>{

    private static final long serialVersionUID = 4262618245950072140L;
    private static Logger logger = LogManager.getLogger(ParseLine.class.getName());

    public String[] call(String line) throws Exception {
        CSVReader reader = new CSVReader(new StringReader(line), '\t');
        String[] vals = reader.readNext();

        logger.info("Parsed to : " + vals.length + " value(s)");
        reader.close();
        return vals;
    }
}
