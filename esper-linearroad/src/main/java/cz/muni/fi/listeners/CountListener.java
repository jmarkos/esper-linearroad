package cz.muni.fi.listeners;

import com.espertech.esper.client.EPRuntime;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;
import cz.muni.fi.Benchmark;
import cz.muni.fi.eventtypes.CountEvent;
import cz.muni.fi.eventtypes.InitialAverageSpeedEvent;
import org.apache.log4j.Logger;

/**
 * Some segments, mainly at the beginning of the simulation, have no traffic, so no
 * statistics events are created for them. But we need them for the Toll query
 * (using outer joins doesn't work, because outer joins don't wait for the (possibly) null side
 * of the join - which means that if the left side comes before the right side, it passes right through, but
 * we want it to wait so we use inner join and therefore need statistics also for non-traffic segments.
 */
public class CountListener implements UpdateListener {

    private static org.apache.log4j.Logger log = Logger.getLogger(CountListener.class);

    private final EPRuntime cepRT;

    public CountListener(EPRuntime cepRT) {
        this.cepRT = cepRT;
    }

    @Override
    public void update(EventBean[] newEvents, EventBean[] oldEvents) {
        log.info("Number of active segments: " + newEvents.length);

        // east=0, west=1, first 100 is east, 100-200 is west
        boolean[][] existingStats = new boolean[Benchmark.NUM_XWAYS][200];
        int min = 0;
        for (EventBean newEvent : newEvents) {
            if (((Double)newEvent.get("min")).intValue() == -1) {
                continue;
                // sometimes esper generates "empty" events, i.e. with min=null, count=0 but with actual xway,seg,dir set,
                // so we just skip those and create them properly afterwards (with min != null)
                // TODO find out the real cause / report a bug
            }
            CountEvent countEvent = new CountEvent(newEvent);
            min = countEvent.min;
            existingStats[countEvent.xway][countEvent.segment + countEvent.direction*100] = true;
            log.debug("Sending count " + countEvent);
            cepRT.sendEvent(countEvent);
        }
        int numEmpty = 0;
        for (int i = 0; i < Benchmark.NUM_XWAYS; i++) {
            // east
            for (int j = 0; j < 100; j++) {
                if (! existingStats[i][j]) {
                    CountEvent countEvent = new CountEvent(min, i, 0, j, 0);
                    log.debug("Sending empty count " + countEvent);
                    cepRT.sendEvent(countEvent);
                    numEmpty++;
                }
            }
            // west
            for (int j = 100; j < 200; j++) {
                if (! existingStats[i][j]) {
                    CountEvent countEvent = new CountEvent(min, i, 1, j - 100, 0);
                    log.debug("Sending empty count " + countEvent);
                    cepRT.sendEvent(countEvent);
                    numEmpty++;
                }
            }
        }
        log.info("Sent " + numEmpty + " empty count events.");
    }

}
