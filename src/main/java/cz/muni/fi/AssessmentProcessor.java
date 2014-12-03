package cz.muni.fi;

import java.util.HashMap;
import org.apache.log4j.Logger;

/**
 *
 * ChangedSegment with no old segment (= new car), just assess the toll
 * ChangedSegment with existing old segment
 *
 * TODO rename this
 */
public class AssessmentProcessor {

    private static org.apache.log4j.Logger log = Logger.getLogger(AssessmentProcessor.class);

    // vid, toll
    HashMap<Integer, Integer> currentTolls; // the tolls which will be added later
    // vid, tolls
    HashMap<Integer, Balance> accountBalances; // running day total

    public AssessmentProcessor() {
        accountBalances = new HashMap<>();
        currentTolls = new HashMap<>();
    }

    public void rememberToll(int vid, int toll) {
        currentTolls.put(vid, toll);
    }

    // Normally this should be called only by the real CHS events (not the ones created when the car is entering the xway),
    // so there should be a previous toll, but if there was an accident there won't
    // TODO dalsi dovod volat toto priamo ked je CHS event, ze to nesuvisi s accidentmi...
    public void assessToll(int vid, short updateTime) {
        Balance newBalance = new Balance();
        newBalance.balance = 0;
        newBalance.lastUpdated = updateTime;
        if (accountBalances.containsKey(vid)) {
            newBalance.balance += accountBalances.get(vid).balance;
        }
        if (currentTolls.containsKey(vid)) {
            newBalance.balance += currentTolls.get(vid);
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
            return result;
        }
        return result;
    }

    class Balance {
        int balance;
        short lastUpdated;
    }

}
