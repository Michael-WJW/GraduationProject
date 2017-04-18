package com.michael.utils;

/**
 * Created by hadoop on 17-4-15.
 */
public class DistanceLocation implements Comparable<DistanceLocation> {
    private double distance;
    private long localtion;
    public DistanceLocation(double distance, long localtion) {
        this.distance = distance;
        this.localtion = localtion;
    }

    @Override
    public int compareTo(DistanceLocation o) {
        if (this.distance > o.distance) {
            return 1;
        } else if (this.distance == o.distance) {
            return 0;
        } else {
            return -1;
        }
    }

    @Override
    public String toString() {
        return "DistanceLocation{" +
                "distance=" + distance +
                ", localtion=" + localtion +
                '}';
    }

    public long getLocaltion() {
        return localtion;
    }

    public void setLocaltion(long localtion) {
        this.localtion = localtion;
    }

    public double getDistance() {
        return distance;
    }

    public void setDistance(double distance) {
        this.distance = distance;
    }
}
