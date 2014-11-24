package cz.muni.fi.listeners;

import com.espertech.esper.client.EPRuntime;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;
import cz.muni.fi.eventtypes.AccidentEvent;
import org.apache.log4j.Logger;

public class AccidentListener implements UpdateListener {

    private static org.apache.log4j.Logger log = Logger.getLogger(AccidentListener.class);

    private EPRuntime cepRT;

    public AccidentListener(EPRuntime cepRT) {
        this.cepRT = cepRT;
    }

    @Override
    public void update(EventBean[] newEvents, EventBean[] oldEvents) {
        if (newEvents == null) {
            return;
        }
        for (EventBean accident : newEvents) {
            int min = ((Double)newEvents[0].get("min")).intValue();
            byte xway = (byte)accident.get("xway");
            byte direction = (byte)accident.get("direction");
            byte segment = (byte)accident.get("segment");
            int position = (int)accident.get("position");
            // EAST, moving to the right, so we need to notify the upstream segments on the left (lower segment)
            if (direction == 0) {
                for (int i = 0; i < 5; i++) {
                    if (segment - i < 0) {
                        break;
                    }
                    AccidentEvent accidentEvent = new AccidentEvent(min, xway, direction, (byte) (segment - i), position);
                    log.debug("Sending accident: " + accidentEvent);
                    cepRT.sendEvent(accidentEvent);
                }
            }

            // WEST, moving to the left, notifying upstream segments on the right (higher segment)
            if (direction == 1) {
                for (int i = 0; i < 5; i++) {
                    if (segment + i > 99) {
                        break;
                    }
                    AccidentEvent accidentEvent = new AccidentEvent(min, xway, direction, (byte) (segment + i), position);
                    log.debug("Sending accident: " + accidentEvent);
                    cepRT.sendEvent(accidentEvent);
                }
            }
        }
    }

}
