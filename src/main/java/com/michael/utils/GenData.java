package com.michael.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by hadoop on 17-4-18.
 */
public class GenData {
    private static final byte[] GLOBAL_TABLE_NAME = "GlobalDataBulkLoad".getBytes();
    private static final byte[] REGION_TABLE_NAME = "RegionDataBulkLoad".getBytes();
    private static final byte[] FAMILY_NAME = "info".getBytes();
    private static final byte[] LATITUDE = "latitude".getBytes();
    private static final byte[] LONGITUDE = "longitude".getBytes();
    private static final byte[] GEOHASHSTR = "geohashstr".getBytes();
    public static void main(String[] args) throws IOException {
//        writeToGlobalDataBulkLoad();
//        writeToIndexDataBulkLoad();

//        Configuration conf = HBaseConfiguration.create();
//        Connection connection = ConnectionFactory.createConnection(conf);
    }

    private static void writeToIndexDataBulkLoad() throws IOException {
        String readPath = new String("/home/hadoop/毕设/数据/GlobalDataBulkLoad-1-1500wan.txt");
        String writePath = new String("/home/hadoop/毕设/数据/IndexDataBulkLoad-1-1500wan.txt");
//        String readPath = "/home/hdfs/download/GlobalDataBulkLoad-1-1500wan.txt";
//        String writePath = "/home/hdfs/download/IndexDataBulkLoad-1-1500wan.txt";
        File file = new File(writePath);
        FileWriter fw = new FileWriter(file, true);
        BufferedWriter bw = new BufferedWriter(fw);
        PrintWriter pw = new PrintWriter(bw);
        if (!file.exists()) {
            file.createNewFile();
        }
        BufferedReader br = new BufferedReader(new FileReader(new File(readPath)));
        String s = null;
        int count = 1;
        while ((s = br.readLine()) != null) {
            count ++;
            String[] words = s.split("\t");
            String rowKey = words[3] + "_" + words[0];
            pw.println(rowKey);
            if (count % 1000000 == 0) {
                System.out.println(count / 1000000);
            }
        }
        br.close();
        pw.close();
    }
    private static void writeToGlobalDataBulkLoad() throws IOException {
        String path = new String("/home/hadoop/毕设/数据/GlobalDataBulkLoad-1-1500wan.txt");
//        String path = new String("/home/hdfs/download/GlobalDataBulkLoad-1-1500wan.txt");
        File file = new File(path);
        FileWriter fw = new FileWriter(file, true);
        BufferedWriter bw = new BufferedWriter(fw);
        PrintWriter pw = new PrintWriter(bw);
        if (!file.exists()) {
            file.createNewFile();
        }
        double latitude, longitude;
        String geohashstr;
        long startTime = System.currentTimeMillis();
        int count = 1;
        for (int i = 0; i < 14999999; i ++) {
            latitude = randomLatitude();
            longitude = randomLongitude();
            geohashstr = GeoHash.withBitPrecision(latitude, longitude, 40).toBase32();
            int rowKey = count % 32;
            String actRowKey = null;
            if (rowKey < 10) {
                actRowKey = "0" + rowKey;
            } else {
                actRowKey = "" + rowKey;
            }
            pw.println(actRowKey + count ++ + "\t" + latitude + "\t" + longitude + "\t" + geohashstr);
            if (i % 1000000 == 0) {
                System.out.println(i / 1000000);
            }
        }
        pw.close();
        long finishTime1 = System.currentTimeMillis();
        System.out.println(finishTime1 - startTime);
    }
    private static double randomLatitude() {
        double result = ((Math.random() * 2) - 1) * 90;
        return result;
    }
    private static double randomLongitude() {
        double result = ((Math.random() * 2) - 1) * 180;
        return result;
    }
    //向表中写入数据，插入1000１-15000的数据 则startId :10001, endKey:15000
    public static void putToTable(int startId, int endId, Connection connection, boolean ifRegion) throws IOException {
        TableName tableName = null;
        if (ifRegion) {
            tableName = TableName.valueOf(REGION_TABLE_NAME);
        } else {
            tableName = TableName.valueOf(GLOBAL_TABLE_NAME);
        }
        Table table = connection.getTable(tableName);
        List<Put> putList = new ArrayList<Put>();
        String actRowKey = null;
        for (int id = startId; id <= endId; id ++) {
            int rowKey = id % 32;
            if (rowKey < 10) {
                actRowKey = "0" + rowKey + id;
            } else {
                actRowKey = "" + rowKey + id;
            }
            double latitude = randomLatitude();
            double longitude = randomLongitude();
            GeoHash geoHash = GeoHash.withBitPrecision(latitude, longitude, 40);
            byte[] geohashstr = Bytes.toBytes(geoHash.toBase32());
            if (ifRegion) {//如果是向regionData表中插入数据的话
                actRowKey = CreateRegionTable.getGeohashInitialStr(geoHash) + "|" + actRowKey;
            }
            Put put = new Put(Bytes.toBytes(actRowKey));
            byte[] actLatitude = CreateGlobalTable.double2Bytes(latitude);
            byte[] actLongitude = CreateGlobalTable.double2Bytes(longitude);
            put.addColumn(FAMILY_NAME, LATITUDE, actLatitude);
            put.addColumn(FAMILY_NAME, LONGITUDE, actLongitude);
            put.addColumn(FAMILY_NAME, GEOHASHSTR, geohashstr);
            putList.add(put);
        }
        table.put(putList);
    }
}
