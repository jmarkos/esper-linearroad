package cz.muni.fi.listeners;

import com.espertech.esper.client.EPRuntime;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;
import cz.muni.fi.Benchmark;
import cz.muni.fi.eventtypes.AverageSpeedEvent;
import cz.muni.fi.eventtypes.InitialAverageSpeedEvent;
import org.apache.log4j.Logger;

public class AverageSpeedListener implements UpdateListener {

    private static org.apache.log4j.Logger log = Logger.getLogger(AverageSpeedListener.class);

    private final EPRuntime cepRT;

    public AverageSpeedListener(EPRuntime cepRT) {
        this.cepRT = cepRT;
    }

    @Override
    public void update(EventBean[] newEvents, EventBean[] oldEvents) {
        for (EventBean newEvent : newEvents) {
            AverageSpeedEvent averageSpeedEvent = new AverageSpeedEvent(newEvent);
            log.debug("Sending average speed " + averageSpeedEvent);
            cepRT.sendEvent(averageSpeedEvent);
        }
    }

}
