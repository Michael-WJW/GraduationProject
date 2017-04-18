package com.michael.utils;

import com.michael.controller.KNNQueryController;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by hadoop on 17-4-17.
 */
public class CreateRegionTable {
    private static final byte[] TABLE_NAME = "RegionData".getBytes();
    private static final byte[] INDEX_FAMILY_NAME = "index".getBytes();
    private static final byte[] FAMILY_NAME = "info".getBytes();
    private static final byte[] LATITUDE = "latitude".getBytes();
    private static final byte[] LONGITUDE = "longitude".getBytes();
    private static final byte[] GEOHASHSTR = "geohashstr".getBytes();

    private static final List<Long> SPLITS = new ArrayList<Long>();
    public static boolean createTable(Admin admin, HTableDescriptor table, byte[][] splits)
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
    //32个region:initial:0x0800000000000000L increment:0x0800000000000000L numRegions:32
    public static byte[][] getRegionSplits(long initial, long increment, int numRegions) {
        byte[][] splits = new byte[numRegions - 1][];
        for (int i = 0; i < numRegions - 1; i ++) {
            splits[i] = GeoHash.longToGeoHash(initial).getBytes();
            initial += increment;
        }
        return splits;
    }
    public static byte[][] get32RegionSplits() {
        return getRegionSplits(0x0800000000000000L, 0x0800000000000000L, 32);
    }
    public static void initialSplits(long initial, long increment, int numRegions) {
        for (int i = 0; i < numRegions; i ++) {
            SPLITS.add(initial);
            initial += increment;
        }
    }
    public static void initial32Splits() {
        initialSplits(0L, 0x0800000000000000l, 32);
    }
    static {
        initial32Splits();
    }

    public static void dataImport(Connection connection) throws IOException {
        String className = "com.michael.coprocessor.RegionIndex";
        KNNQueryController.setCoprocessor(className, true);

        TableName tableName = TableName.valueOf(TABLE_NAME);
        Table table = connection.getTable(tableName);
        String path = new String("/home/hadoop/毕设/数据/data1.txt");
        FileReader fr = new FileReader(new File(path));
        BufferedReader br = new BufferedReader(fr);
        int count = 1;
        String s = null;
        List<Put> putList = new ArrayList<Put>(5001);
        while (true) {
            while ((s = br.readLine()) != null) {
                String[] data = s.split(" ");
                double latitude = Double.parseDouble(data[0]);
                double longitude = Double.parseDouble(data[1]);
                GeoHash geoHash = GeoHash.withBitPrecision(latitude, longitude, 40);
                long bits = geoHash.getGeoHashBits();
                long bitsCopy = bits;
                bits >>>= 59;
                byte[] rowKeyByte = (GeoHash.longToGeoHash(SPLITS.get((int)bits)) + "|" + count ++).getBytes();
                Put put = new Put(rowKeyByte);
                put.addColumn(FAMILY_NAME, LATITUDE, CreateGlobalTable.double2Bytes(latitude));
                put.addColumn(FAMILY_NAME, LONGITUDE, CreateGlobalTable.double2Bytes(longitude));
                put.addColumn(FAMILY_NAME, GEOHASHSTR, data[2].getBytes());
                putList.add(put);
                if (putList.size() == 5000) {
                    System.out.println("size: " + putList.size());
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
    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);
        HBaseAdmin admin = new HBaseAdmin(conf);

        //建表语句
//        HTableDescriptor descriptor = new HTableDescriptor(TABLE_NAME);
//        HColumnDescriptor family = new HColumnDescriptor(FAMILY_NAME);
//        descriptor.addFamily(family);
//        HColumnDescriptor index_family = new HColumnDescriptor(INDEX_FAMILY_NAME);
//        descriptor.addFamily(index_family);
//        byte[][] spilts = get32RegionSplits();
//        createTable(admin, descriptor, spilts);

        //向表中插入数据语句
        dataImport(connection);
    }
}
