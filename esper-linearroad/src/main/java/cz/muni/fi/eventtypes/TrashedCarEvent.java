package cz.muni.fi.eventtypes;

public class TrashedCarEvent {

    public short time;
    public int vid;
    public byte xway;
    public byte lane;
    public byte direction;
    public byte segment;
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
