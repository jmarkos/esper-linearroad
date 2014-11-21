package cz.muni.fi.eventtypes;

import com.espertech.esper.client.EventBean;

public class AverageSpeedStats implements Comparable {

    public byte xway; // 0..L-1
    public byte direction; // 0 = East, 1 = West
    public byte segment; // 0..99, 1 mile long
    public double averageSpeed;

    public AverageSpeedStats(int xway, int direction, int segment, double averageSpeed) {
        this.xway = (byte) xway;
        this.direction = (byte) direction;
        this.segment = (byte) segment;
        this.averageSpeed = averageSpeed;
    }

    public AverageSpeedStats(EventBean e) {
        this.xway = (byte)e.get("xway");
        this.direction = (byte)e.get("direction");
        this.segment = (byte)e.get("segment");
        // in some cases the stats can give us avgspd=null (in cases where count was 0)
        if (e.get("averageSpeed") != null) {
            this.averageSpeed = (double)e.get("averageSpeed");
        } else {
            this.averageSpeed = 0; // doesn't matter what we set, if count is <40 there is no toll anyway
            System.out.println("Average speed was null! " + this.toString());
        }
    }

    @Override
    public int compareTo(Object o) {
        AverageSpeedStats m = (AverageSpeedStats) o;
        if (this.xway != m.xway)
            return this.xway - m.xway;
        if (this.direction != m.direction)
            return this.direction - m.direction;
        return this.segment - m.segment;
    }

    @Override
    public String toString() {
        return "eventtypes.SegStat{" +
                "xway=" + xway +
                ", direction=" + direction +
                ", segment=" + segment +
                ", averageSpeed=" + averageSpeed +
                '}';
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

    public double getAverageSpeed() {
        return averageSpeed;
    }

    public void setAverageSpeed(double averageSpeed) {
        this.averageSpeed = averageSpeed;
    }

}
