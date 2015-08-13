package com.demo.SparkKafkaStream;

import java.util.List;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Time;

/*
 * table in HBase
 * create 'page_views' ,'views'
 */

public class SaveToHBase implements Function2<JavaRDD<String[]>, Time, Void> {

    private static Logger logger = LogManager.getLogger(SaveToHBase.class.getName());
    private static final long serialVersionUID = 4232618245650972140L;
    private static HBaseUtils hUtils = new HBaseUtils();

    private static String hTable = "page_views";
    private static String colFamily = "views";
    private static String colName = "total_views";

    public Void call(JavaRDD<String[]> rdd, Time time) throws Exception {

        List<String[]> vals = rdd.collect();
        for (String[] eachRow:vals) {
            String url = eachRow[2].toString();

			logger.info("Process URL: " + url);
			try {
				String curVal = hUtils.table_get(hTable, url, colFamily, colName);
				logger.info("Current Value for: " + url + " = " + curVal);
			} catch(Exception ex) {
				logger.info("Exception : " + ex.toString());
			}
        }

        return null;
    }
}
