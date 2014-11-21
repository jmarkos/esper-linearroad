package cz.muni.fi.eventtypes;


// The same as PositionReport, but speed is always 0
public class StoppedCarEvent implements Event {

    public byte type;
    public short time; // 0..10799 seconds
    public int vid; // in L = 1 as high as 130 000
    public byte speed; // 0..100 mph
    public byte xway; // 0..L-1
    public byte lane; // 0..4, 0 = entrance, 4 = exit, 1-3 drive lanes
    public byte direction; // 0 = East, 1 = West
    public byte segment; // 0..99, 1 mile long
    public int position; /// 0..527999 feet

    public StoppedCarEvent(PositionReportEvent pre) {
        this.type = pre.type;
        this.time = pre.time;
        this.vid = pre.vid;
        this.speed = pre.speed;
        this.xway = pre.xway;
        this.lane = pre.lane;
        this.direction = pre.direction;
        this.segment = pre.segment;
        this.position = pre.position;
    }

    public StoppedCarEvent(String[] properties) {
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

    @Override
    public String toString() {
        return "StoppedCarEvent{" +
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
