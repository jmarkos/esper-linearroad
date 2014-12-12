package cz.muni.fi.eventtypes;

import com.espertech.esper.client.EventBean;

public class AccidentEvent {

    public int min; // minute for which the stats were computed, max 180
    public byte xway; // 0..L-1
    public byte direction; // 0 = East, 1 = West
    public byte segment; // 0..99, 1 mile long
    public byte originalSegment; // the segment where the accident truly occured
    public int position;

    public AccidentEvent(int min, byte xway, byte direction, byte segment, byte originalSegment, int position) {
        this.min = min;
        this.xway = xway;
        this.direction = direction;
        this.segment = segment;
        this.originalSegment = originalSegment;
        this.position = position;
    }

    public AccidentEvent(EventBean e) {
        this.min = ((Double)e.get("min")).intValue();
        this.xway = (byte)e.get("xway");
        this.direction = (byte)e.get("direction");
        this.segment = (byte)e.get("segment");
        this.originalSegment = (byte)e.get("originalSegment");
        this.position = (int)e.get("position");
    }

    @Override
    public String toString() {
        return "eventtypes.AccidentEvent{" +
                "min=" + min +
                ", xway=" + xway +
                ", direction=" + direction +
                ", segment=" + segment +
                ", originalSegment=" + originalSegment +
                ", position=" + position +
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

    public byte getOriginalSegment() {
        return originalSegment;
    }

    public void setOriginalSegment(byte originalSegment) {
        this.originalSegment = originalSegment;
    }

    public int getPosition() {
        return position;
    }

    public void setPosition(int position) {
        this.position = position;
    }

}
