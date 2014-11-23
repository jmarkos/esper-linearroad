package cz.muni.fi.listeners;

import com.espertech.esper.client.EPRuntime;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;
import cz.muni.fi.eventtypes.ChangedSegmentEvent;

public class ChangedSegmentListener implements UpdateListener {

    private EPRuntime cepRT;

    public ChangedSegmentListener(EPRuntime cepRT) {
        this.cepRT = cepRT;
    }

    @Override
    public void update(EventBean[] newEvents, EventBean[] oldEvents) {
        if (newEvents.length > 1) {
            System.out.println("Something is wrong?");
        }
        System.out.println(new ChangedSegmentEvent(
                (short)newEvents[0].get("time"),
                (int)newEvents[0].get("vid"),
                (byte)newEvents[0].get("xway"),
                (byte)newEvents[0].get("lane"),
                (byte)newEvents[0].get("direction"),
                (byte)newEvents[0].get("oldSegment"),
                (byte)newEvents[0].get("newSegment")));
        cepRT.sendEvent(new ChangedSegmentEvent(
                (short) newEvents[0].get("time"),
                (int) newEvents[0].get("vid"),
                (byte) newEvents[0].get("xway"),
                (byte) newEvents[0].get("lane"),
                (byte) newEvents[0].get("direction"),
                (byte) newEvents[0].get("oldSegment"),
                (byte) newEvents[0].get("newSegment")));
    }

}
