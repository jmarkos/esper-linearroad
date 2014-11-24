package cz.muni.fi.eventtypes;

import com.espertech.esper.client.EventBean;

public class TollEvent {

    public int min; // minute for which the stats were computed, max 180
    public byte xway; // 0..L-1
    public byte direction; // 0 = East, 1 = West
    public byte segment; // 0..99, 1 mile long
    public double averageSpeed;
    public long count;
    public boolean accident = false;
    public long toll;

    public TollEvent(int min, byte xway, byte direction, byte segment, double averageSpeed, long count, boolean accident, long toll) {
        this.min = min;
        this.xway = xway;
        this.direction = direction;
        this.segment = segment;
        this.averageSpeed = averageSpeed;
        this.count = count;
        this.accident = accident;
        this.toll = toll;
    }

    public TollEvent(EventBean e) {
        this.min = (int)e.get("min");
        this.xway = (byte)e.get("xway");
        this.direction = (byte)e.get("direction");
        this.segment = (byte)e.get("segment");
        this.averageSpeed = (double)e.get("averageSpeed");
        this.count = (long)e.get("count");
        // anything other than null means there was an accident
        if (e.get("accident") != null) {
            this.accident = true;
        }
        this.toll = computeToll();
    }

    public long computeToll() {
        if (accident || count <= 50 || averageSpeed >= 40) {
            return 0;
        }
        return (long) (2 * Math.pow((count - 50), 2));
    }

    @Override
    public String toString() {
        return "TollEvent{" +
                "min=" + min +
                ", xway=" + xway +
                ", direction=" + direction +
                ", segment=" + segment +
                ", averageSpeed=" + averageSpeed +
                ", count=" + count +
                ", accident=" + accident +
                ", toll=" + toll +
                '}';
    }

    public int getMin() {
        return min;
    }

    public void setMin(int min) {
        this.min = min;
    }

    public byte getXway() {
        return xway;
    }

    public void setXway(byte xway) {
        this.xway = xway;
    }

    public byte getDirection() {
        return direction;
    }

    public void setDirection(byte direction) {
        this.direction = direction;
    }

    public byte getSegment() {
        return segment;
    }

    public void setSegment(byte segment) {
        this.segment = segment;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public double getAverageSpeed() {
        return averageSpeed;
    }

    public void setAverageSpeed(double averageSpeed) {
        this.averageSpeed = averageSpeed;
    }

    public boolean isAccident() {
        return accident;
    }

    public void setAccident(boolean accident) {
        this.accident = accident;
    }

    public long getToll() {
        return toll;
    }

    public void setToll(long toll) {
        this.toll = toll;
    }
}
