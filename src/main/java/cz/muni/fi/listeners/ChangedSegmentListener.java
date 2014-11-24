package cz.muni.fi.listeners;

import com.espertech.esper.client.EPRuntime;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;
import cz.muni.fi.eventtypes.ChangedSegmentEvent;
import org.apache.log4j.Logger;

public class ChangedSegmentListener implements UpdateListener {

    private static org.apache.log4j.Logger log = Logger.getLogger(ChangedSegmentListener.class);

    private EPRuntime cepRT;

    public ChangedSegmentListener(EPRuntime cepRT) {
        this.cepRT = cepRT;
    }

    @Override
    public void update(EventBean[] newEvents, EventBean[] oldEvents) {
        if (newEvents.length > 1) {
            log.warn("Changed segment listener received more than 1 new events.");
        }
        for (EventBean newEvent : newEvents) {
            ChangedSegmentEvent changedSegmentEvent = new ChangedSegmentEvent(newEvent);
            log.debug("Sending changed segment " + changedSegmentEvent);
            cepRT.sendEvent(changedSegmentEvent);
        }
    }

}
