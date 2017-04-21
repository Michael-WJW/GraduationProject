package com.michael.utils;

import java.io.*;

/**
 * Created by hadoop on 17-4-18.
 */
public class GenData {
    public static void main(String[] args) throws IOException {
//        writeToGlobalDataBulkLoad();
//        writeToIndexDataBulkLoad();
    }

    private static void writeToIndexDataBulkLoad() throws IOException {
//        String readPath = new String("/home/hadoop/毕设/数据/GlobalDataBulkLoad-2000001-15000000.txt");
//        String writePath = new String("/home/hadoop/毕设/数据/IndexDataBulkLoad-2000001-15000000.txt");
        String readPath = "/home/hdfs/download/GlobalDataBulkLoad-2000001-15000000.txt";
        String writePath = "/home/hdfs/download/IndexDataBulkLoad-2000001-15000000.txt";
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
//        String path = new String("/home/hadoop/毕设/数据/GlobalDataBulkLoad-2000001-15000000.txt");
        String path = new String("/home/hdfs/download/GlobalDataBulkLoad-2000001-15000000.txt");
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
        int count = 2000001;
        for (int i = 0; i < 13000000; i ++) {
            latitude = randomLatitude();
            longitude = randomLongitude();
            geohashstr = GeoHash.withBitPrecision(latitude, longitude, 40).toBase32();
            pw.println(count ++ + "\t" + latitude + "\t" + longitude + "\t" + geohashstr);
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
}
