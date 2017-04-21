package com.michael.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;

/**
 * Created by hadoop on 17-4-18.
 */
public class TestHBase {
    private static final byte[] TABLE_NAME = "GlobalDataTest".getBytes();
    private static final byte[] FAMILY_NAME = "info".getBytes();
    public static void createTable(HBaseAdmin admin) throws IOException {
        if (!admin.tableExists(TABLE_NAME)) {
            HTableDescriptor tableDescriptor = new HTableDescriptor(TABLE_NAME);
            HColumnDescriptor family = new HColumnDescriptor(FAMILY_NAME);
            tableDescriptor.addFamily(family);
            admin.createTable(tableDescriptor);
        }
    }

    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);
        HBaseAdmin admin = new HBaseAdmin(conf);
        createTable(admin);
    }
}
