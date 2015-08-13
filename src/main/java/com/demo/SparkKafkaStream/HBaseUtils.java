package com.demo.SparkKafkaStream;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class HBaseUtils {

    private static Logger logger = LogManager.getLogger(HBaseUtils.class.getName());

    private Configuration hConfig;
    private HConnection connection;
    private HTableInterface theTable;

    public HBaseUtils() {
        try {
            hConfig = HBaseConfiguration.create();
            hConfig.set("hbase.zookeeper.quorum", "maprdemo");
            hConfig.set("hbase.zookeeper.property.clientPort", "5181");
            hConfig.set("hbase.rootdir", "maprfs:///hbase");
            hConfig.set("hbase.cluster.distributed", "true");
            hConfig.set("dfs.support.append", "true");
            hConfig.set("hbase.fsutil.maprfs.impl", "org.apache.hadoop.hbase.util.FSMapRUtils");

            connection = HConnectionManager.createConnection(hConfig);
        } catch (Exception ex) {
            logger.info("Exception while init: " + ex.toString());
        }
    }

    public void table_put(String tableName, String rowKey, String colFamily,
                          String col, String val) throws Exception {

        logger.info("Put on: " + tableName + " -> " + rowKey + " " + colFamily + ":" + col + " = " + val);
        theTable = connection.getTable(tableName);
        Put p = new Put(Bytes.toBytes(rowKey));
        p.add(Bytes.toBytes(colFamily), Bytes.toBytes(col),Bytes.toBytes(val));
        theTable.put(p);
        theTable.close();
    }

    public String table_get(String tableName, String rowKey, String colFamily,
            String col) throws Exception {

        logger.info("Get on: " + tableName + " -> " + rowKey + " " + colFamily + ":" + col);
        theTable = connection.getTable(tableName);
        Get g = new Get(Bytes.toBytes(rowKey));
        Result r = theTable.get(g);
        byte[] value = r.getValue(Bytes.toBytes(colFamily), Bytes.toBytes(col));
        String strVal = Bytes.toString(value);
        theTable.close();
        return strVal;
    }
}
