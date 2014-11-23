package cz.muni.fi.eventtypes;

public class ChangedSegmentEvent {

    public short time; // 0..10799 seconds
    public int min;
    public int vid; // in L = 1 as high as 130 000
    public byte xway; // 0..L-1
    public byte lane; // 0..4, 0 = entrance, 4 = exit, 1-3 drive lanes
    public byte direction; // 0 = East, 1 = West
    public byte oldSegment;
    public byte newSegment;

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

    public int computeMinute(int seconds) {
        return (int) Math.floor(seconds / 60) + 1;
    }

    @Override
    public String toString() {
        return "ChangedSegmentEvent{" +
                "time=" + time +
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
