package com.demo.SparkKafkaStream;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class HBaseUtils {

	private static Logger logger = LogManager.getLogger(ParseLine.class.getName());

    private Configuration hConfig;

    public HBaseUtils() {
        hConfig = HBaseConfiguration.create();
        hConfig.addResource(new Path("file:///opt/mapr/hbase/hbase-0.98.12/conf/hbase-site.xml"));
    }

    public void table_put(String tableName, String rowKey, String colFamily,
                          String col, String val) throws Exception {

		logger.info("Put on: " + tableName + " -> " + rowKey + " " + colFamily + ":" + col + " = " + val);
        HTable table = new HTable(hConfig, tableName);
        Put p = new Put(Bytes.toBytes(rowKey));
        p.add(Bytes.toBytes(colFamily), Bytes.toBytes(col),
                Bytes.toBytes(val));
        table.put(p);
        table.close();
    }

    public String table_get(String tableName, String rowKey, String colFamily,
            String col) throws Exception {

		logger.info("Get on: " + tableName + " -> " + rowKey + " " + colFamily + ":" + col);
        HTable table = new HTable(hConfig, tableName);
        Get g = new Get(Bytes.toBytes(rowKey));
        Result r = table.get(g);
        byte[] value = r.getValue(Bytes.toBytes(colFamily), Bytes
                .toBytes(col));
        String strVal = Bytes.toString(value);
        table.close();
        return strVal;
    }
}
