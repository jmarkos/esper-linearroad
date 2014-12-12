package cz.muni.fi.eventtypes;

public class DailyExpenditureResponse {

    public byte type = 3;
    public short time;
    public short outputTime;
    public int qid;
    public int balance;

    public DailyExpenditureResponse(short time, int qid, int balance) {
        this.time = time;
        this.qid = qid;
        this.balance = balance;
    }

    @Override
    public String toString() {
        return "DailyExpenditureResponse{" +
                "type=" + type +
                ", time=" + time +
                ", outputTime=" + outputTime +
                ", qid=" + qid +
                ", balance=" + balance +
                '}';
    }

    public String toFileString() {
        StringBuilder result = new StringBuilder();
        result.append(type + ",");
        result.append(time + ",");
        result.append(outputTime + ",");
        result.append(qid + ",");
        result.append(balance);
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

    public int getQid() {
        return qid;
    }

    public void setQid(int qid) {
        this.qid = qid;
    }

    public int getBalance() {
        return balance;
    }

    public void setBalance(int balance) {
        this.balance = balance;
    }
}
