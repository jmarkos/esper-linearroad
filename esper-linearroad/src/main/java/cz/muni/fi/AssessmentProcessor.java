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

    // <vid> -> <toll>
    ConcurrentHashMap<Integer, Integer> pendingTolls;
    // <vid> -> <tolls, lastUpdated>
    ConcurrentHashMap<Integer, Balance> accountBalances; // running day total

    public AssessmentProcessor() {
        accountBalances = new ConcurrentHashMap<Integer, Balance>();
        pendingTolls = new ConcurrentHashMap<Integer, Integer>();
    }

    public void rememberToll(int vid, int toll) {
        pendingTolls.put(vid, toll);
    }

    public void assessToll(int vid, short updateTime) {
        Balance newBalance = new Balance();
        newBalance.balance = 0;
        newBalance.lastUpdated = updateTime;
        if (accountBalances.containsKey(vid)) {
            newBalance.balance += accountBalances.get(vid).balance;
        }
        // if the previous segment was affected by accident, there was no toll charged
        if (pendingTolls.containsKey(vid)) {
            newBalance.balance += pendingTolls.get(vid);
            pendingTolls.remove(vid);
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

    public class Balance {
        public int balance;
        public short lastUpdated;
    }

}
