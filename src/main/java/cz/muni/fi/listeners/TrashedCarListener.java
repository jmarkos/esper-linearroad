package cz.muni.fi.listeners;

import com.espertech.esper.client.EPRuntime;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;
import cz.muni.fi.eventtypes.LRBEvent;
import cz.muni.fi.eventtypes.TrashedCarEvent;

public class TrashedCarListener implements UpdateListener {

    private EPRuntime cepRT;

    public TrashedCarListener(EPRuntime cepRT) {
        this.cepRT = cepRT;
    }

    @Override
    public void update(EventBean[] newEvents, EventBean[] oldEvents) {
        TrashedCarEvent tce = new TrashedCarEvent();
        tce.time = ((LRBEvent) newEvents[0].get("pr3")).time;
        tce.vid = ((LRBEvent) newEvents[0].get("pr3")).vid;
        tce.xway = ((LRBEvent) newEvents[0].get("pr3")).xway;
        tce.segment = ((LRBEvent) newEvents[0].get("pr3")).segment;
        tce.direction = ((LRBEvent) newEvents[0].get("pr3")).direction;
        tce.position = ((LRBEvent) newEvents[0].get("pr3")).position;
        cepRT.sendEvent(tce);
//        System.out.println("tce = " + tce);
    }

}
