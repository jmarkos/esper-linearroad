package cz.muni.fi.listeners;

import com.espertech.esper.client.EPRuntime;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;
import cz.muni.fi.Benchmark;
import cz.muni.fi.eventtypes.InitialAverageSpeedEvent;
import org.apache.log4j.Logger;

public class InitialAverageSpeedListener implements UpdateListener {

    private static org.apache.log4j.Logger log = Logger.getLogger(InitialAverageSpeedListener.class);

    private final EPRuntime cepRT;

    public InitialAverageSpeedListener(EPRuntime cepRT) {
        this.cepRT = cepRT;
    }

    @Override
    public void update(EventBean[] newEvents, EventBean[] oldEvents) {
        log.info("Number of active segments: " + newEvents.length);
        // we need to list through and find all missing segments (segments which didn't have any action), and produce
        // an 'empty' statistic event for them, so that the downstream query can use length(5)

        // east=0, west=1, first 100 is east, 100-200 is west
        boolean[][] existingStats = new boolean[Benchmark.NUM_XWAYS][200];
        int min = 0;
        for (EventBean newEvent : newEvents) {
            if (((Double)newEvent.get("min")).intValue() == -1) {
                continue;
                // sometimes esper generates "empty" events, i.e. with min=null, averageSpeed=null but with actual xway,seg,dir set,
                // so we just skip those and create them properly afterwards (with min and averageSpeed != null)
            }
            InitialAverageSpeedEvent averageSpeedEvent = new InitialAverageSpeedEvent(newEvent);
            min = averageSpeedEvent.min;
            existingStats[averageSpeedEvent.xway][averageSpeedEvent.segment + averageSpeedEvent.direction*100] = true;
            log.debug("Sending initial average speed " + averageSpeedEvent);
            cepRT.sendEvent(averageSpeedEvent);
        }
        int numEmpty = 0;
        for (int i = 0; i < Benchmark.NUM_XWAYS; i++) {
            // east
            for (int j = 0; j < 100; j++) {
                if (! existingStats[i][j]) {
                    InitialAverageSpeedEvent ias = new InitialAverageSpeedEvent(min, i, 0, j, 0);
                    log.debug("Sending empty initial average speed " + ias);
                    cepRT.sendEvent(ias);
                    numEmpty++;
                }
            }
            // west
            for (int j = 100; j < 200; j++) {
                if (! existingStats[i][j]) {
                    InitialAverageSpeedEvent ias = new InitialAverageSpeedEvent(min, i, 1, j - 100, 0);
                    log.debug("Sending empty initial average speed " + ias);
                    cepRT.sendEvent(ias);
                    numEmpty++;
                }
            }
        }
        log.info("Sent " + numEmpty + " empty initial average speed events.");

    }

}
