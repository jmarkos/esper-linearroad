package cz.muni.fi.eventtypes;

import com.espertech.esper.client.EventBean;

public class AccidentNotificationEvent {

    public byte type; // =1
    public short time; // time of the position report which triggered this
    public short outputTime;
    public int vid;
    public int segment;

    public AccidentNotificationEvent(EventBean e) {
        this.type = 1;
        this.time = (short)e.get("time");
        this.vid = (int)e.get("vid");
        this.segment = (int)e.get("accSegment");
    }

    @Override
    public String toString() {
        return "AccidentNotificationEvent{" +
                "segment=" + segment +
                ", outputTime=" + outputTime +
                ", time=" + time +
                ", vid=" + vid +
                ", type=" + type +
                '}';
    }

    // LRB paper says the output tuple should have 4 fields, but the validator
    // creates a table with 5 (with vid)
    public String toFileString() {
        StringBuilder result = new StringBuilder();
        result.append(type + ",");
        result.append(time + ",");
        result.append(outputTime + ",");
        result.append(vid + ",");
        result.append(segment);
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

    public int getSegment() {
        return segment;
    }

    public void setSegment(int segment) {
        this.segment = segment;
    }

    public int getVid() {
        return vid;
    }

    public void setVid(int vid) {
        this.vid = vid;
    }
}
