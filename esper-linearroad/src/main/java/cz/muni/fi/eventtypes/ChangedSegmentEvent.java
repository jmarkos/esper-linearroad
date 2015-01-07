package cz.muni.fi.eventtypes;

import com.espertech.esper.client.EventBean;

public class ChangedSegmentEvent {

    public short time;
    public int min;
    public int vid;
    public byte xway;
    public byte lane;
    public byte direction;
    public byte oldSegment;
    public byte newSegment;

    public ChangedSegmentEvent() {
    }

    public ChangedSegmentEvent(short time, int vid, byte xway, byte lane, byte direction, byte oldSegment, byte newSegment) {
        this.time = time;
        this.min = computeMinute(time) - 1; // we are interested in the toll for the new segment from previous minute
        this.vid = vid;
        this.xway = xway;
        this.lane = lane;
        this.direction = direction;
        this.oldSegment = oldSegment;
        this.newSegment = newSegment;
    }

    public ChangedSegmentEvent(PositionReportEvent pre) {
        this.time = pre.time;
        this.min = computeMinute(this.time) - 1;
        this.vid = pre.vid;
        this.xway = pre.xway;
        this.lane = pre.lane;
        this.direction = pre.direction;
        this.newSegment = pre.segment;
        this.oldSegment = -1;
    }
    
    public ChangedSegmentEvent(EventBean e) {
        this.time = (short) e.get("time");
        this.min = computeMinute(this.time) - 1;
        this.vid = (int) e.get("vid");
        this.xway = (byte) e.get("xway");
        this.lane = (byte) e.get("lane");
        this.direction = (byte) e.get("direction");
        this.oldSegment = (byte) e.get("oldSegment");
        this.newSegment = (byte) e.get("newSegment");
    }

    public int computeMinute(int seconds) {
        return (int) Math.floor(seconds / 60) + 1;
    }

    @Override
    public String toString() {
        return "ChangedSegmentEvent{" +
                "time=" + time +
                ", min=" + min +
                ", vid=" + vid +
                ", xway=" + xway +
                ", lane=" + lane +
                ", direction=" + direction +
                ", oldSegment=" + oldSegment +
                ", newSegment=" + newSegment +
                '}';
    }

    public short getTime() {
        return time;
    }

    public void setTime(short time) {
        this.time = time;
    }

    public int getMin() {
        return min;
    }

    public void setMin(int min) {
        this.min = min;
    }

    public int getVid() {
        return vid;
    }

    public void setVid(int vid) {
        this.vid = vid;
    }

    public byte getXway() {
        return xway;
    }

    public void setXway(byte xway) {
        this.xway = xway;
    }

    public byte getLane() {
        return lane;
    }

    public void setLane(byte lane) {
        this.lane = lane;
    }

    public byte getDirection() {
        return direction;
    }

    public void setDirection(byte direction) {
        this.direction = direction;
    }

    public byte getOldSegment() {
        return oldSegment;
    }

    public void setOldSegment(byte oldSegment) {
        this.oldSegment = oldSegment;
    }

    public byte getNewSegment() {
        return newSegment;
    }

    public void setNewSegment(byte newSegment) {
        this.newSegment = newSegment;
    }
}
