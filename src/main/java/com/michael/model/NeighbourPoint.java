package com.michael.model;

import com.michael.utils.GeoHash;

/**
 * direction 方向解释
 * 0:最初的方格
 * 1:北
 * 2:北
 * 3:东
 * 4:东南
 * 5:南
 * 6:西南
 * 7:西
 * 8:西北
 */
public class NeighbourPoint {
    public GeoHash geoHashStart;
    public GeoHash geoHashEnd;
    public int direction;
    public int level;
    public NeighbourPoint(GeoHash geoHashStart, GeoHash geoHashEnd, int direction, int level) {
        this.geoHashStart = geoHashStart;
        this.geoHashEnd = geoHashEnd;
        this.direction = direction;
        this.level = level;
    }
}
