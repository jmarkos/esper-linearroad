package cz.muni.fi.listeners;

import com.espertech.esper.client.EPRuntime;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;
import cz.muni.fi.Benchmark;
import cz.muni.fi.eventtypes.CountEvent;
import cz.muni.fi.eventtypes.InitialAverageSpeedEvent;
import org.apache.log4j.Logger;

public class CountListener implements UpdateListener {

    private static org.apache.log4j.Logger log = Logger.getLogger(CountListener.class);

    private final EPRuntime cepRT;

    public CountListener(EPRuntime cepRT) {
        this.cepRT = cepRT;
    }

    @Override
    public void update(EventBean[] newEvents, EventBean[] oldEvents) {
        System.out.println("count listener! " + newEvents.length);
        // we need to list through and find all missing segments (segments which didn't have any action), and produce
        // an 'empty' statistic event for them, so that we can later do an inner join
        boolean[][] existingStats = new boolean[Benchmark.NUM_XWAYS][200];
        int min = 0;
        // east=0, west=1, first 100 is east, 100-200 is west
        for (EventBean newEvent : newEvents) {
            if (((Double)newEvent.get("min")).intValue() == -1) {
                continue;
                // sometimes esper generates "empty" events, i.e. with min=null, count=0 but with actual xway,seg,dir set,
                // so we just skip those and create them properly afterwards (with min != null)
            }
            CountEvent countEvent = new CountEvent(newEvent);
            min = countEvent.min;
            existingStats[countEvent.xway][countEvent.segment + countEvent.direction*100] = true;
            log.debug("Sending " + countEvent);
            cepRT.sendEvent(countEvent);
        }
        for (int i = 0; i < Benchmark.NUM_XWAYS; i++) {
            // east
            for (int j = 0; j < 100; j++) {
                if (! existingStats[i][j]) {
                    cepRT.sendEvent(new CountEvent(min, i, 0, j, 0));
                }
            }
            // west
            for (int j = 100; j < 200; j++) {
                if (! existingStats[i][j]) {
                    cepRT.sendEvent(new CountEvent(min, i, 1, j - 100, 0));
                }
            }
        }

    }

}
