package cz.muni.fi.eventtypes;

import static java.lang.String.format;

/**
 * LRB event/tuple type
 */
public class LRBEvent {

    public byte type;
    public short time; // 0..10799 seconds
    public int vid; // in L = 1 as high as 130 000
    public byte speed; // 0..100 mph
    public byte xway; // 0..L-1
    public byte lane; // 0..4, 0 = entrance, 4 = exit, 1-3 drive lanes
    public byte direction; // 0 = East, 1 = West
    public byte segment; // 0..99, 1 mile long
    public int position; /// 0..527999 feet
    public int qid; // query id
    public byte sinit; // 0..99, start segment
    public byte send; // 0..99, end segment
    public byte dow; // 1..7, day of week
    public short tod; // 0..1440, time of day
    // intentionally not called 'day', it's a reserved keyword in esper
    public byte dayy; // 1..69, 1 = yesterday, 69 = 10 weeks ago

    // TODO outputTime ?

    @Override
    public String toString() {
        return "LRBEvent{" +
                "type=" + type +
                ", time=" + time +
                ", vid=" + vid +
                ", speed=" + speed +
                ", xway=" + xway +
                ", lane=" + lane +
                ", direction=" + direction +
                ", segment=" + segment +
                ", position=" + position +
                ", qid=" + qid +
                ", sinit=" + sinit +
                ", send=" + send +
                ", dow=" + dow +
                ", tod=" + tod +
                ", dayy=" + dayy +
                '}';
    }

    // faster?
//    @Override
//    public String toString() {
//        return String.format("type: %d, time: %d, vid: %d, speed: %d, xway: %d, lane: %d, direction: %d, segment: %d, position: %d, qid: %d, sinit: %d, send: %d, dow: %d, tod: %d, dayy: %d",
//                type, time, vid, speed, xway, lane, direction, segment, position, qid, sinit, send, dow, tod, dayy);
//    }

    // setters and getters are needed by Esper

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

    public int getQid() {
        return qid;
    }

    public void setQid(int qid) {
        this.qid = qid;
    }

    public byte getSinit() {
        return sinit;
    }

    public void setSinit(byte sinit) {
        this.sinit = sinit;
    }

    public byte getSend() {
        return send;
    }

    public void setSend(byte send) {
        this.send = send;
    }

    public byte getDow() {
        return dow;
    }

    public void setDow(byte dow) {
        this.dow = dow;
    }

    public short getTod() {
        return tod;
    }

    public void setTod(short tod) {
        this.tod = tod;
    }

    public byte getDayy() {
        return dayy;
    }

    public void setDayy(byte dayy) {
        this.dayy = dayy;
    }

}
