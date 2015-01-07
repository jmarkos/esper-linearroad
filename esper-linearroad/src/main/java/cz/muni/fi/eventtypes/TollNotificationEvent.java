package cz.muni.fi.eventtypes;

import com.espertech.esper.client.EventBean;

public class TollNotificationEvent {

    // lrb: type, vid, time, emittime, avgspeed, toll
    public byte type; // =0
    public short time;
    public short outputTime;
    public int vid;
    public int averageSpeed; // validator expects integer
    public int toll;

    public TollNotificationEvent(EventBean e) {
        this.type = 0;
        this.time = (short)e.get("time");
        // outputTime set by OutputWriter
        this.vid = (int)e.get("vid");
        this.averageSpeed = ((Double)e.get("averageSpeed")).intValue();
        this.toll = (int)e.get("toll");
    }

    @Override
    public String toString() {
        return "TollNotificationEvent{" +
                "type=" + type +
                ", time=" + time +
                ", outputTime=" + outputTime +
                ", vid=" + vid +
                ", averageSpeed=" + averageSpeed +
                ", toll=" + toll +
                '}';
    }

    public String toFileString() {
        StringBuilder result = new StringBuilder();
        result.append(type + ",");
        result.append(vid + ",");
        result.append(time + ",");
        result.append(outputTime + ",");
        result.append(averageSpeed + ",");
        result.append(toll);
        return result.toString();
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

    public short getOutputTime() {
        return outputTime;
    }

    public void setOutputTime(short outputTime) {
        this.outputTime = outputTime;
    }

    public int getVid() {
        return vid;
    }

    public void setVid(int vid) {
        this.vid = vid;
    }

    public int getAverageSpeed() {
        return averageSpeed;
    }

    public void setAverageSpeed(int averageSpeed) {
        this.averageSpeed = averageSpeed;
    }

    public int getToll() {
        return toll;
    }

    public void setToll(int toll) {
        this.toll = toll;
    }
}

