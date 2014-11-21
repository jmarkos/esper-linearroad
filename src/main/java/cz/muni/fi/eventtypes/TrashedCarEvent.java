package cz.muni.fi.eventtypes;

public class TrashedCarEvent {

    public short time; // 0..10799 seconds
    public int vid; // in L = 1 as high as 130 000
    public byte xway; // 0..L-1
    public byte lane; // 0..4, 0 = entrance, 4 = exit, 1-3 drive lanes
    public byte direction; // 0 = East, 1 = West
    public byte segment; // 0..99, 1 mile long
    public int position;

    public TrashedCarEvent(short time, int vid, byte xway, byte lane, byte direction, byte segment, int position) {
        this.time = time;
        this.vid = vid;
        this.xway = xway;
        this.lane = lane;
        this.direction = direction;
        this.segment = segment;
        this.position = position;
    }

    public TrashedCarEvent() {

    }

    @Override
    public String toString() {
        return "eventtypes.TrashedCarEvent{" +
                "time=" + time +
                ", vid=" + vid +
                ", xway=" + xway +
                ", lane=" + lane +
                ", direction=" + direction +
                ", segment=" + segment +
                ", position=" + position +
                '}';
    }

    public short getTime() {
        return time;
    }

    public void setTime(short time) {
        this.time = time;
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

    public byte getSegment() {
        return segment;
    }

    public void setSegment(byte segment) {
        this.segment = segment;
    }

    public int getPosition() {
        return position;
    }

    public void setPosition(int position) {
        this.position = position;
    }
}
