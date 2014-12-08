package cz.muni.fi;

import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

/**
 *
 * For every ChangedSegmentEvent, we need to assess the toll for the previous segment.
 * For every TollNotificationEvent, we need to remember the toll charged for the new segment.
 *
 * TODO rename this
 */
public class AssessmentProcessor {

    private static org.apache.log4j.Logger log = Logger.getLogger(AssessmentProcessor.class);

    // <vid, xway> -> <toll>
    ConcurrentHashMap<CarKey, Integer> pendingTolls;
    // <vid> -> <tolls, lastUpdated>
    ConcurrentHashMap<Integer, Balance> accountBalances; // running day total

    public AssessmentProcessor() {
        accountBalances = new ConcurrentHashMap<Integer, Balance>();
        pendingTolls = new ConcurrentHashMap<CarKey, Integer>();
    }

    public void rememberToll(int vid, int xway, int toll) {
        pendingTolls.put(new CarKey(vid, xway), toll);
    }

    public void assessToll(int vid, int xway, short updateTime) {
        Balance newBalance = new Balance();
        newBalance.balance = 0;
        newBalance.lastUpdated = updateTime;
        if (accountBalances.containsKey(vid)) {
            newBalance.balance += accountBalances.get(vid).balance;
        }
        CarKey ck = new CarKey(vid, xway);
        // if the previous segment was affected by accident, there was no toll charged
        if (pendingTolls.containsKey(ck)) {
            newBalance.balance += pendingTolls.get(ck);
            pendingTolls.remove(ck);
        }
        accountBalances.put(vid, newBalance);
    }

    // there can be queries asking a balance of cars which had no tolls yet
    public Balance getBalance(int vid) {
        Balance result = accountBalances.get(vid);
        if (result == null) {
            result = new Balance();
            result.balance = 0;
            // TODO
            result.lastUpdated = -1;
        }
        return result;
    }

    class Balance {
        int balance;
        short lastUpdated;
    }

    class CarKey {

        int vid;
        int xway;

        public CarKey(int vid, int xway) {
            this.vid = vid;
            this.xway = xway;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            CarKey carKey = (CarKey) o;

            if (vid != carKey.vid) return false;
            if (xway != carKey.xway) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = vid;
            result = 31 * result + xway;
            return result;
        }
    }

}
