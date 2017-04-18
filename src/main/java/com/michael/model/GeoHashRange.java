package com.michael.model;

/**
 * Created by hadoop on 17-4-17.
 */
public class GeoHashRange {
    public long leftDown;
    public long rightUp;
    public int sameBits;
    public GeoHashRange(long leftDown, long rightUp, int sameBits) {
        this.leftDown = leftDown;
        this.rightUp = rightUp;
        this.sameBits = sameBits;
    }
}
