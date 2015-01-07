package cz.muni.fi.eventtypes;

public class AccountBalanceQuery implements Event {

    public byte type;
    public short time;
    public int vid;
    public int qid;

    public AccountBalanceQuery() {
    }

    public AccountBalanceQuery(String[] properties) {
        this.type = Byte.valueOf(properties[0]);
        this.time = Short.valueOf(properties[1]);
        this.vid = Integer.valueOf(properties[2]);
        this.qid = Integer.valueOf(properties[9]);
    }
    
    @Override
    public String toString() {
        return "AccountBalanceQuery{" +
                "type=" + type +
                ", time=" + time +
                ", vid=" + vid +
                ", qid=" + qid +
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

    public int getQid() {
        return qid;
    }

    public void setQid(int qid) {
        this.qid = qid;
    }
}
