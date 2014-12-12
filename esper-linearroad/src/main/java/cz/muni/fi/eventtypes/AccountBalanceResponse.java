package cz.muni.fi.eventtypes;

public class AccountBalanceResponse {

    public byte type = 2;
    public short time;
    public short outputTime;
    public int qid;
    public int balance;
    public int resultTime; // time when balance was last updated

    public AccountBalanceResponse(short time, int qid, int balance, int resultTime) {
        this.time = time;
        this.qid = qid;
        this.balance = balance;
        this.resultTime = resultTime;
    }

    @Override
    public String toString() {
        return "AccountBalanceResponse{" +
                "type=" + type +
                ", time=" + time +
                ", outputTime=" + outputTime +
                ", qid=" + qid +
                ", balance=" + balance +
                ", resultTime=" + resultTime +
                '}';
    }

    // the LRB paper says the order should be type, time, outputTime, resultTime, qid, balance
    // but the validator creates a table      type, time, outputTime, qid, resultTime, balance
    public String toFileString() {
        StringBuilder result = new StringBuilder();
        result.append(type + ",");
        result.append(time + ",");
        result.append(outputTime + ",");
        result.append(qid + ",");
        result.append(resultTime + ",");
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

    public int getResultTime() {
        return resultTime;
    }

    public void setResultTime(int resultTime) {
        this.resultTime = resultTime;
    }
}
