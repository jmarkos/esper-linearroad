package cz.muni.fi.listeners;

import com.espertech.esper.client.EPRuntime;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;
import cz.muni.fi.eventtypes.LRBEvent;
import cz.muni.fi.eventtypes.StoppedCarEvent;
import cz.muni.fi.eventtypes.TrashedCarEvent;
import org.apache.log4j.Logger;

public class TrashedCarListener implements UpdateListener {

    private static org.apache.log4j.Logger log = Logger.getLogger(TrashedCarListener.class);

    private EPRuntime cepRT;

    public TrashedCarListener(EPRuntime cepRT) {
        this.cepRT = cepRT;
    }

    @Override
    public void update(EventBean[] newEvents, EventBean[] oldEvents) {
        TrashedCarEvent tce = new TrashedCarEvent();
        tce.time = ((StoppedCarEvent) newEvents[0].get("pr3")).time;
        tce.vid = ((StoppedCarEvent) newEvents[0].get("pr3")).vid;
        tce.xway = ((StoppedCarEvent) newEvents[0].get("pr3")).xway;
        tce.segment = ((StoppedCarEvent) newEvents[0].get("pr3")).segment;
        tce.direction = ((StoppedCarEvent) newEvents[0].get("pr3")).direction;
        tce.position = ((StoppedCarEvent) newEvents[0].get("pr3")).position;
        log.debug("Sending trashed car " + tce);
        cepRT.sendEvent(tce);
    }

}
