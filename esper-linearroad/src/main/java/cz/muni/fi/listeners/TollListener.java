package cz.muni.fi.listeners;

import com.espertech.esper.client.EPRuntime;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;
import cz.muni.fi.eventtypes.TollEvent;
import org.apache.log4j.Logger;

public class TollListener implements UpdateListener {

    private static org.apache.log4j.Logger log = Logger.getLogger(TollListener.class);

    private EPRuntime cepRT;

    public TollListener(EPRuntime cepRT) {
        this.cepRT = cepRT;
    }

    @Override
    public void update(EventBean[] newEvents, EventBean[] oldEvents) {
        for (EventBean newEvent : newEvents) {
            TollEvent tollEvent = new TollEvent(newEvent);
            log.debug("Sending toll " + tollEvent);
            cepRT.sendEvent(tollEvent);
        }
    }

}
