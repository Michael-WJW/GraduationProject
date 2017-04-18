package com.michael.utils;

import java.io.*;

/**
 * Created by hadoop on 17-4-18.
 */
public class GenData {
    public static void main(String[] args) throws IOException {
        String path = new String("/home/hadoop/毕设/数据/data1.txt");
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
        for (int i = 0; i < 20000; i ++) {
            latitude = randomLatitude();
            longitude = randomLongitude();
            geohashstr = GeoHash.withBitPrecision(latitude, longitude, 40).toBase32();
            pw.println(latitude + " " + longitude + " " + geohashstr);
            if (i % 1000 == 0) {
                System.out.println(i / 1000);
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
