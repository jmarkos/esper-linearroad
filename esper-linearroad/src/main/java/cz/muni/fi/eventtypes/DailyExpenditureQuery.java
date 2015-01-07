package cz.muni.fi.eventtypes;

public class DailyExpenditureQuery implements Event {

    // file historicalTolls has structure: vid, day, xway, tolls

    public byte type;
    public short time;
    public int vid;
    public byte xway;
    public int qid;
    // intentionally not called 'day', it's a reserved keyword in esper
    public byte dayy;

    public DailyExpenditureQuery() {
    }

    public DailyExpenditureQuery(String[] properties) {
        this.type = Byte.valueOf(properties[0]);
        this.time = Short.valueOf(properties[1]);
        this.vid = Integer.valueOf(properties[2]);
        this.xway = Byte.valueOf(properties[4]);
        this.qid = Integer.valueOf(properties[9]);
        this.dayy = Byte.valueOf(properties[14]);
    }

    public DailyExpenditureQuery(byte type, short time, int vid, byte xway, int qid, byte dayy) {
        this.type = type;
        this.time = time;
        this.vid = vid;
        this.xway = xway;
        this.qid = qid;
        this.dayy = dayy;
    }

    @Override
    public String toString() {
        return "DailyExpenditureQuery{" +
                "type=" + type +
                ", time=" + time +
                ", vid=" + vid +
                ", xway=" + xway +
                ", qid=" + qid +
                ", dayy=" + dayy +
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

    public byte getXway() {
        return xway;
    }

    public void setXway(byte xway) {
        this.xway = xway;
    }

    public int getQid() {
        return qid;
    }

    public void setQid(int qid) {
        this.qid = qid;
    }

    public byte getDayy() {
        return dayy;
    }

    public void setDayy(byte dayy) {
        this.dayy = dayy;
    }
}
