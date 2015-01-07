package cz.muni.fi.eventtypes;

public class PositionReportEvent implements Event {

    public byte type;
    public short time;
    public int vid;
    public byte speed;
    public byte xway;
    public byte lane;
    public byte direction;
    public byte segment;
    public int position;

    public PositionReportEvent() {
    }

    public PositionReportEvent(String[] properties) {
        this.type = Byte.valueOf(properties[0]);
        this.time = Short.valueOf(properties[1]);
        this.vid = Integer.valueOf(properties[2]);
        this.speed = Byte.valueOf(properties[3]);
        this.xway = Byte.valueOf(properties[4]);
        this.lane = Byte.valueOf(properties[5]);
        this.direction = Byte.valueOf(properties[6]);
        this.segment = Byte.valueOf(properties[7]);
        this.position = Integer.valueOf(properties[8]);
    }

    // just for testing
    public PositionReportEvent(short time, int vid, byte segment) {
        this.time = time;
        this.vid = vid;
        this.segment = segment;
    }

    @Override
    public String toString() {
        return "PositionReportEvent{" +
                "type=" + type +
                ", time=" + time +
                ", vid=" + vid +
                ", speed=" + speed +
                ", xway=" + xway +
                ", lane=" + lane +
                ", direction=" + direction +
                ", segment=" + segment +
                ", position=" + position +
                '}';
    }

    public byte getType() {
        return type;
    }

    public void setType(byte type) {
        this.type = type;
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

    public byte getSpeed() {
        return speed;
    }

    public void setSpeed(byte speed) {
        this.speed = speed;
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
