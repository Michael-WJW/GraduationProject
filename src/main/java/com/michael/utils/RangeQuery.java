package com.michael.utils;

import com.michael.model.GeoHashRange;

import java.util.ArrayDeque;
import java.util.Deque;

/**
 * Created by hadoop on 17-4-3.
 */
public class RangeQuery {
    private static final long firstBitMask = 0x8000000000000000l;
//    public static Deque<GeoHashRange> query(double leftDownLatitude, double leftDownLongitude, double rightUplatitude, double rightUplongitude) {
//        return query(leftDownLatitude, leftDownLongitude, rightUplatitude, rightUplongitude, 15);
//    }
    public static Deque<GeoHashRange> query(double leftDownLatitude, double leftDownLongitude, double rightUplatitude, double rightUplongitude) {
        GeoHash geoLeftDown = GeoHash.withBitPrecision(leftDownLatitude, leftDownLongitude, 40);
        GeoHash geoRightUp = GeoHash.withBitPrecision(rightUplatitude, rightUplongitude, 40);
        long leftDown = geoLeftDown.getGeoHashBits();
        long rightUp = geoRightUp.getGeoHashBits();
        int initSameBits = getSameBits(leftDown, rightUp);
        Deque<GeoHashRange> deque = new ArrayDeque<GeoHashRange>();
        GeoHashRange range = new GeoHashRange(leftDown, rightUp, initSameBits);
        deque.offer(range);
//        System.out.println("initSameBits: " + initSameBits);
        if (initSameBits >= 35) {
            return deque;
        }
        int bits = initSameBits + 5;
        while (deque.size() > 0 && deque.peek().sameBits < bits) {
            GeoHashRange first = deque.poll();
            if (getOneBit(first.leftDown, first.sameBits + 1) == getOneBit(first.rightUp, first.sameBits + 1)) {
                first.sameBits ++;
                deque.offer(first);
            } else {
                long leftDownTemp = first.leftDown;
                long rightUpTemp = first.rightUp;

                long leftDownToRightUp = changeRestBitsTo1(leftDownTemp, first.sameBits + 1);
                leftDownToRightUp = leftDownToRightUp | rightUpTemp;
                leftDownToRightUp = setOneBitTo0(leftDownToRightUp, first.sameBits + 1);
                GeoHashRange range1 = new GeoHashRange(leftDownTemp, leftDownToRightUp, first.sameBits + 1);
                deque.offer(range1);

                long rightUpToLeftDown = changeRestBitsTo1(rightUpTemp, first.sameBits + 1);
                rightUpToLeftDown = rightUpToLeftDown | leftDownTemp;
                rightUpToLeftDown = changeRestBitsTo0(rightUpToLeftDown, first.sameBits + 1);
                GeoHashRange range2 = new GeoHashRange(rightUpToLeftDown, rightUpTemp, first.sameBits + 1);
                deque.offer(range2);
            }
        }
        return deque;
    }
    private static long changeRestBitsTo1(long leftDown, int bitNow) {
        long temp = 0;
        int j = 0;
        for (int i = 0; i < 64; i ++) {
            if (i < bitNow) {
                int firstBit = (leftDown & firstBitMask) != 0 ? 1 : 0;
                leftDown <<= 1;
                temp <<= 1;
                if (firstBit == 1) {
                    temp = temp | 0x1;
                }
            } else {
                if (j % 2 == 0) {
                    temp <<= 1;
                } else {
                    temp <<= 1;
                    temp = temp | 0x1;
                }
                j ++;
            }
        }
        return temp;
    }
    private static long changeRestBitsTo0(long rightUp, int bitNow) {
        long temp = 0;
        int j = 0;
        for (int i = 0; i < 64; i ++) {
            if (i < bitNow || j % 2 == 0) {
                int firstBit = (rightUp & firstBitMask) != 0 ? 1 : 0;
                rightUp <<= 1;
                temp <<= 1;
                if (firstBit == 1) {
                    temp = temp | 0x1;
                }
                if (i >= bitNow) {
                    j ++;
                }
            } else {
                rightUp <<= 1;
                temp <<= 1;
                j ++;
            }
        }
        return temp;
    }
    private static long setOneBitTo0(long l, int bit) {
        long bitMask = 1;
        bitMask = bitMask << (64 - bit);
        bitMask = ~bitMask;
        l = l & bitMask;
        return l;
    }
    public static int getOneBit(long l, int bit) {
        long bitMask = 1;
        bitMask = bitMask << (64 - bit);
        if ((bitMask & l) == 0) {
            return 0;
        } else {
            return 1;
        }
    }
    private static int getSameBits(long bits1, long bits2) {
        int sameBits = 0;
        if (bits1 == bits2) {
            return 64;
        }
        while ( ( (bits1 & firstBitMask) == firstBitMask && (bits2 & firstBitMask) == firstBitMask ) ||
                ( (bits1 & firstBitMask) == 0 && (bits2 & firstBitMask) == 0) ) {
            sameBits ++;
            bits1 = bits1 << 1;
            bits2 = bits2 << 1;
        }
        return sameBits;
    }
    public static void main(String[] args) {
//        double latitude = -79.0;
//        double longtide = -179.0;
//        long firstFiveBitsMask = 0x0000000000000000l;
//        GeoHash geoHash = GeoHash.withBitPrecision(latitude, longtide, 40);
//        System.out.println(geoHash.getGeoHashBits() & firstFiveBitsMask);
//        System.out.println(geoHash.toBase32());
//        Deque<Integer> deque = new ArrayDeque<>();
//        deque.offer(1);
//        deque.offer(2);
//        deque.offer(3);
//        deque.poll();
//        System.out.println(deque.peek());
//        System.out.println(deque.getFirst());
//        firstFiveBitsMask = changeRestBitsTo1(firstFiveBitsMask, 4);
////        firstFiveBitsMask = setOneBitTo0(firstFiveBitsMask, 6);
//        for (int i = 1; i <= 64; i ++) {
//            System.out.print(getOneBit(firstFiveBitsMask, i));
//        }
//        System.out.println();

//        long leftDownTemp = 0x0400000000000000l;
//        long rightUpTemp = 0x3400000000000000l;
//        long rightUpToLeftDown = changeRestBitsTo1(rightUpTemp, 3);
//        rightUpToLeftDown = rightUpToLeftDown | leftDownTemp;
//        rightUpToLeftDown = changeRestBitsTo0(rightUpToLeftDown, 3);
//        for (int i = 1; i <= 64; i ++) {
//            System.out.print(getOneBit(rightUpToLeftDown, i));
//        }
//        System.out.println();
        double leftDownLatitude, leftDownLongitude, rightUpLatitude, rightUpLongitude;
        leftDownLatitude = 23.50;
        leftDownLongitude = 73.5;
        rightUpLatitude = 30.0;
        rightUpLongitude = 80.0;
        GeoHash leftDown = GeoHash.withBitPrecision(leftDownLatitude, leftDownLongitude, 40);
        GeoHash rightUp = GeoHash.withBitPrecision(rightUpLatitude, rightUpLongitude, 40);
        long leftDownBits = leftDown.getGeoHashBits();
        long rightUpBits = rightUp.getGeoHashBits();

        System.out.println(getSameBits(leftDownBits, rightUpBits));

        for (int i = 1; i <= 64; i ++) {
            System.out.print(getOneBit(leftDownBits, i));
        }
        System.out.println();

        for (int i = 1; i <= 64; i ++) {
            System.out.print(getOneBit(rightUpBits, i));
        }
        System.out.println();

        Deque<GeoHashRange> deque = query(leftDownLatitude, leftDownLongitude, rightUpLatitude, rightUpLongitude);
        System.out.println(deque.size());
        char c1 = '_' + 1;//'`'
        char c2 = '_' - 1;//'^'
        System.out.println(c1);
    }
}
