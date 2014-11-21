package cz.muni.fi.eventtypes;

import com.espertech.esper.client.EventBean;

public class CountStats implements Comparable {

    public byte xway; // 0..L-1
    public byte direction; // 0 = East, 1 = West
    public byte segment; // 0..99, 1 mile long
    public long count;

    public CountStats(int xway, int direction, int segment, long count) {
        this.xway = (byte) xway;
        this.direction = (byte) direction;
        this.segment = (byte) segment;
        this.count = count;
    }

    public CountStats(EventBean e) {
        this.xway = (byte)e.get("xway");
        this.direction = (byte)e.get("direction");
        this.segment = (byte)e.get("segment");
        this.count = (long)e.get("count");
    }

    @Override
    public int compareTo(Object o) {
        CountStats m = (CountStats) o;
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
                ", count=" + count +
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

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

}
