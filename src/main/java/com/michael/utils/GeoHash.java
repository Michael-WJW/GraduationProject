package com.michael.utils;

public class GeoHash {
    private static final int MAX_BIT_PRECISION = 64;
    public static final long FIRST_BIT_FLAGGED = 0x8000000000000000L;
    protected byte significantBits = 0;
    protected long bits = 0;
    private static final char[] base32 = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'b', 'c', 'd', 'e', 'f',
            'g', 'h', 'j', 'k', 'm', 'n', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z' };

    public static GeoHash withBitPrecision(double latitude, double longitude, int numberOfBits) {
        if (numberOfBits > MAX_BIT_PRECISION) {
            throw new IllegalArgumentException("A Geohash can only be " + MAX_BIT_PRECISION + " bits long!");
        }
        if (Math.abs(latitude) > 90.0 || Math.abs(longitude) > 180.0) {
            throw new IllegalArgumentException("Can't have lat/lon values out of (-90,90)/(-180/180)");
        }
        return new GeoHash(latitude, longitude, numberOfBits);
    }

    public static GeoHash withDistance(double latitude, double longitude, double distance) {
        if (0 <= distance && distance < 0.05) {
            return withBitPrecision(latitude, longitude, 40);
        } else if (0.05 <= distance && distance < 0.5) {
            return withBitPrecision(latitude, longitude, 34);
        } else if (0.5 <= distance && distance < 1.0) {
            return withBitPrecision(latitude, longitude, 30);
        } else if (1.0 <= distance && distance < 10.0) {
            return withBitPrecision(latitude, longitude, 24);
        } else if (10.0 <= distance && distance < 80.0) {
            return withBitPrecision(latitude, longitude, 20);
        } else if (80.0 <= distance && distance < 200.0) {
            return withBitPrecision(latitude, longitude, 14);
        } else {
            return null;
        }
    }
    private GeoHash(double latitude, double longitude, int desiredPrecision) {
        desiredPrecision = Math.min(desiredPrecision, MAX_BIT_PRECISION);

        boolean isEvenBit = true;
        double[] latitudeRange = { -90, 90 };
        double[] longitudeRange = { -180, 180 };
        //以desiredPrecision为60为例
        while (significantBits < desiredPrecision) {//偶数位放经度（例如0），奇数位放纬度（例如1）
            if (isEvenBit) {
                divideRangeEncode(longitude, longitudeRange);
            } else {
                divideRangeEncode(latitude, latitudeRange);
            }
            isEvenBit = !isEvenBit;
        }

        bits <<= (MAX_BIT_PRECISION - desiredPrecision);
    }
    private GeoHash(long bits, byte significantBits) {
        this.bits = bits;
        this.significantBits = significantBits;
    }
    //找东南西北邻居的代码
    private long extractEverySecondBit(long copyOfBits, int numberOfBits) {
        long value = 0;
        for (int i = 0; i < numberOfBits; i++) {
            if ((copyOfBits & FIRST_BIT_FLAGGED) == FIRST_BIT_FLAGGED) {
                value |= 0x1;
            }
            value <<= 1;
            copyOfBits <<= 2;
        }
        value >>>= 1;
        return value;
    }
    protected int[] getNumberOfLatLonBits() {
        if (significantBits % 2 == 0) {
            return new int[] { significantBits / 2, significantBits / 2 };
        } else {
            return new int[] { significantBits / 2, significantBits / 2 + 1 };
        }
    }
    private long maskLastNBits(long value, long n) {
        long mask = 0xffffffffffffffffl;
        mask >>>= (MAX_BIT_PRECISION - n);
        return value & mask;
    }
    private long getMaskAll1(long n) {
        long mask = 0xffffffffffffffffl;
        mask >>>= (MAX_BIT_PRECISION - n);
        return mask;
    }
    public GeoHash getNorthernNeighbour() {
        long[] latitudeBits = getRightAlignedLatitudeBits();
        //latitudeBits[0]:value, latitudeBits[1]:value的位数
        long[] longitudeBits = getRightAlignedLongitudeBits();
        if (latitudeBits[0] == getMaskAll1(latitudeBits[1])) {
            return null;
        }
        latitudeBits[0] += 1;
        latitudeBits[0] = maskLastNBits(latitudeBits[0], latitudeBits[1]);
        return recombineLatLonBitsToHash(latitudeBits, longitudeBits);
    }
    public GeoHash getSouthernNeighbour() {
        long[] latitudeBits = getRightAlignedLatitudeBits();
        long[] longitudeBits = getRightAlignedLongitudeBits();
        if (latitudeBits[0] == 0) {
            return null;
        }
        latitudeBits[0] -= 1;
        latitudeBits[0] = maskLastNBits(latitudeBits[0], latitudeBits[1]);
        return recombineLatLonBitsToHash(latitudeBits, longitudeBits);
    }
    public GeoHash getEasternNeighbour() {
        long[] latitudeBits = getRightAlignedLatitudeBits();
        long[] longitudeBits = getRightAlignedLongitudeBits();
        if (longitudeBits[0] == getMaskAll1(longitudeBits[1])) {
            return null;
        }
        longitudeBits[0] += 1;
        longitudeBits[0] = maskLastNBits(longitudeBits[0], longitudeBits[1]);
        return recombineLatLonBitsToHash(latitudeBits, longitudeBits);
    }
    public GeoHash getWesternNeighbour() {
        long[] latitudeBits = getRightAlignedLatitudeBits();
        long[] longitudeBits = getRightAlignedLongitudeBits();
        if (longitudeBits[0] == 0) {
            return null;
        }
        longitudeBits[0] -= 1;
        longitudeBits[0] = maskLastNBits(longitudeBits[0], longitudeBits[1]);
        return recombineLatLonBitsToHash(latitudeBits, longitudeBits);
    }
    public GeoHash[] getAdjacent() {
        GeoHash northern = getNorthernNeighbour();
        GeoHash eastern = getEasternNeighbour();
        GeoHash southern = getSouthernNeighbour();
        GeoHash western = getWesternNeighbour();
        return new GeoHash[] { northern, northern.getEasternNeighbour(), eastern, southern.getEasternNeighbour(),
                southern,
                southern.getWesternNeighbour(), western, northern.getWesternNeighbour() };
    }
    public GeoHash recombineLatLonBitsToHash(long[] latBits, long[] lonBits) {
        long longitude = lonBits[0];
        long latitude = latBits[0];
        int longiLength = (int)lonBits[1];
        int latiLength = (int)latBits[1];
        int length = longiLength + latiLength;
        longitude <<= (MAX_BIT_PRECISION - longiLength);
        latitude <<= (MAX_BIT_PRECISION - latiLength);
        long value = 0L;
        for (int i = 0; i < length; i ++) {
            if (i % 2 == 0) {//对经度进行操作
                if ((longitude & FIRST_BIT_FLAGGED) == FIRST_BIT_FLAGGED) {
                    value |= 0x1;
                }
                longitude <<= 1;
            } else {//对纬度进行操作
                if ((latitude & FIRST_BIT_FLAGGED) == FIRST_BIT_FLAGGED) {
                    value |= 0x1;
                }
                latitude <<= 1;
            }
            value <<= 1;
        }
        value >>>= 1;
        value <<= (MAX_BIT_PRECISION - length);
        return new GeoHash(value, (byte)length);
    }
    protected long[] getRightAlignedLatitudeBits() {
        long copyOfBits = bits << 1;
        long value = extractEverySecondBit(copyOfBits, getNumberOfLatLonBits()[0]);
        return new long[] { value, getNumberOfLatLonBits()[0] };
    }
    protected long[] getRightAlignedLongitudeBits() {
        long copyOfBits = bits;
        long value = extractEverySecondBit(copyOfBits, getNumberOfLatLonBits()[1]);
        return new long[] { value, getNumberOfLatLonBits()[1] };
    }
    private void divideRangeEncode(double value, double[] range) {
        double mid = (range[0] + range[1]) / 2;
        if (value >= mid) {
            addOnBitToEnd();
            range[0] = mid;
        } else {
            addOffBitToEnd();
            range[1] = mid;
        }
    }

    //左移１位并将新添加的一位置为１
    protected final void addOnBitToEnd() {
        significantBits++;
        bits <<= 1;
        bits = bits | 0x1;
    }
    //左移１位并将新添加的一位置为0
    protected final void addOffBitToEnd() {
        significantBits++;
        bits <<= 1;
    }

    public String toBase32() {
        StringBuilder buf = new StringBuilder();

        long firstFiveBitsMask = 0xf800000000000000l;
        long bitsCopy = bits;
        int partialChunks = 8;

        for (int i = 0; i < partialChunks; i++) {
            int pointer = (int) ((bitsCopy & firstFiveBitsMask) >>> 59);
            buf.append(base32[pointer]);
            bitsCopy <<= 5;
        }
        return buf.toString();
    }

    public static String longToGeoHash(long bits) {
        StringBuilder buf = new StringBuilder();

        long firstFiveBitsMask = 0xf800000000000000l;
        long bitsCopy = bits;
        int partialChunks = 8;

        for (int i = 0; i < partialChunks; i++) {
            int pointer = (int) ((bitsCopy & firstFiveBitsMask) >>> 59);
            buf.append(base32[pointer]);
            bitsCopy <<= 5;
        }
        return buf.toString();
    }

    public long getGeoHashBits() {
        return this.bits;
    }

    public GeoHash changeRestBitsTo1() {
        long initialBits = this.bits;
        byte signibits = this.significantBits;
        long temp = 0;
        for (int i = 0; i < 64; i ++) {
            if (i < signibits) {
                int firstBit = (initialBits & FIRST_BIT_FLAGGED) != 0 ? 1 : 0;
                initialBits <<= 1;
                if (firstBit == 1) {
                    temp = temp | 0x1;
                }
                if (i < 63) {
                    temp <<= 1;
                }
            } else {
                temp = temp | 0x1;
                if (i < 63) {
                    temp <<= 1;
                }
            }
        }
        return new GeoHash(temp, signibits);
    }

//    public static void main(String[] args) {
//        double latitude = 38.5111;
//        double longitude = -96.8005;
//        int sigi = 34;
//        GeoHash geoHash = withBitPrecision(latitude, longitude, sigi);
//        System.out.println(geoHash.toBase32());
//        System.out.println(geoHash.changeRestBitsTo1().toBase32());
//    }
}

