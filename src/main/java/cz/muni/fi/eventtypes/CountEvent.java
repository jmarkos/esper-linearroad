package cz.muni.fi.eventtypes;

import com.espertech.esper.client.EventBean;

public class CountEvent {

    public int min; // minute for which the stats were computed, max 180
    public byte xway; // 0..L-1
    public byte direction; // 0 = East, 1 = West
    public byte segment; // 0..99, 1 mile long
    public long count;

    public CountEvent(int min, int xway, int direction, int segment, long count) {
        this.min = min;
        this.xway = (byte) xway;
        this.direction = (byte) direction;
        this.segment = (byte) segment;
        this.count = count;
    }

    public CountEvent(EventBean e) {
        this.min = ((Double)e.get("min")).intValue();
        this.xway = (byte)e.get("xway");
        this.direction = (byte)e.get("direction");
        this.segment = (byte)e.get("segment");
        this.count = (long)e.get("count");
    }

    @Override
    public String toString() {
        return "eventtypes.CountEvent{" +
                "min=" + min +
                ", xway=" + xway +
                ", direction=" + direction +
                ", segment=" + segment +
                ", count=" + count +
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

}
