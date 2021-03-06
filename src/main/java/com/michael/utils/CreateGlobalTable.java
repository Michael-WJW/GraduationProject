package com.michael.utils;

import com.michael.controller.KNNQueryController;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.*;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by hadoop on 17-4-18.
 */
public class CreateGlobalTable {
    private static final byte[] TABLE_NAME = "GlobalDataBulkLoad".getBytes();
    private static final byte[] FAMILY_NAME = "info".getBytes();
    private static final byte[] LATITUDE = "latitude".getBytes();
    private static final byte[] LONGITUDE = "longitude".getBytes();
    private static final byte[] GEOHASHSTR = "geohashstr".getBytes();
    private static final byte[]  INDEX_TABLE_NAME = "IndexDataBulkLoad".getBytes();
    private static final byte[]  INDEX_TABLE_FAMILY_NAME = "info".getBytes();

    public static void main(String[] args) throws IOException {
//        Configuration conf = HBaseConfiguration.create();
//        Connection connection = ConnectionFactory.createConnection(conf);
//        HBaseAdmin admin = new HBaseAdmin(conf);
//        createGlobalTable(admin);//创建数据表　32个region
//        createIndexTable(admin);//创建全局索引表 16个region

    }
    public static double bytes2Double(byte[] arr) {
        long value = 0;
        for (int i = 0; i < 8; i++) {
            value |= ((long) (arr[i] & 0xff)) << (8 * i);
        }
        return Double.longBitsToDouble(value);
    }
    public static byte[] double2Bytes(double d) {
        long value = Double.doubleToRawLongBits(d);
        byte[] byteRet = new byte[8];
        for (int i = 0; i < 8; i++) {
            byteRet[i] = (byte) ((value >> 8 * i) & 0xff);
        }
        return byteRet;
    }
    public static void createGlobalTable(HBaseAdmin admin) throws IOException {
        HTableDescriptor tableDescriptor = new HTableDescriptor(TABLE_NAME);
        HColumnDescriptor family = new HColumnDescriptor(FAMILY_NAME);
        tableDescriptor.addFamily(family);
        byte[][] spilts = new byte[31][];
        for (int i = 1; i <= 31; i ++) {
            if (i >= 10) {
                spilts[i - 1] = (i + "").getBytes();
            } else {
                spilts[i - 1] = ("0" + i).getBytes();
            }
        }
        createGlobalTable(admin, tableDescriptor, spilts);
    }
    public static void createIndexTable(HBaseAdmin admin) throws IOException {
        HTableDescriptor tableDescriptor = new HTableDescriptor(INDEX_TABLE_NAME);
        HColumnDescriptor family = new HColumnDescriptor(INDEX_TABLE_FAMILY_NAME);
        tableDescriptor.addFamily(family);
        byte[][] spilts = CreateRegionTable.get16RegionSplits();
        createGlobalTable(admin, tableDescriptor, spilts);
    }
    public static boolean createGlobalTable(Admin admin, HTableDescriptor table, byte[][] splits)
            throws IOException {
        try {
            admin.createTable(table, splits);
            return true;
        } catch (TableExistsException e) {
//            logger.info("table " + table.getNameAsString() + " already exists");
            // the table already exists...
            return false;
        }
    }
    public static void putTableValues(Connection connection) throws IOException {
        String className = "com.michael.coprocessor.GlobalIndex";
        KNNQueryController.setCoprocessor(className, false);

        TableName tableName = TableName.valueOf(TABLE_NAME);
        Table table = connection.getTable(tableName);
        String path = new String("/home/hadoop/毕设/数据/data200wan.txt");
        FileReader fr = new FileReader(new File(path));
        BufferedReader br = new BufferedReader(fr);
        int count = 1;
        int k = 1;
        String s = null;
        List<Put> putList = new ArrayList<Put>(10001);
        while (true) {
            while ((s = br.readLine()) != null) {
                String[] data = s.split(" ");
                byte[] latitude = double2Bytes(Double.parseDouble(data[0]));
                byte[] longitude = double2Bytes(Double.parseDouble(data[1]));
                byte[] geohashstr = data[2].getBytes();
                Put put = new Put((count++ + "").getBytes());
                put.addColumn(FAMILY_NAME, LATITUDE, latitude);
                put.addColumn(FAMILY_NAME, LONGITUDE, longitude);
                put.addColumn(FAMILY_NAME, GEOHASHSTR, geohashstr);
                putList.add(put);
                if (putList.size() == 10000) {
                    System.out.println("k: " + k ++);
                    break;
                }
            }
            table.put(putList);
            putList.clear();
            if (s == null) {
                break;
            }
        }
        System.out.println("count: " + count);
    }
}
