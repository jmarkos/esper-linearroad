package cz.muni.fi.listeners;

import com.espertech.esper.client.EPRuntime;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;
import cz.muni.fi.AssessmentProcessor;
import cz.muni.fi.OutputWriter;
import cz.muni.fi.eventtypes.AccidentNotificationEvent;
import cz.muni.fi.eventtypes.TollNotificationEvent;
import org.apache.log4j.Logger;

public class NotificationListener implements UpdateListener {

    private static org.apache.log4j.Logger log = Logger.getLogger(NotificationListener.class);

    private EPRuntime cepRT;
    private OutputWriter outputWriter;
    private AssessmentProcessor assessmentProcessor;

    public NotificationListener(EPRuntime cepRT, OutputWriter outputWriter, AssessmentProcessor assessmentProcessor) {
        this.cepRT = cepRT;
        this.outputWriter = outputWriter;
        this.assessmentProcessor = assessmentProcessor;
    }

    @Override
    public void update(EventBean[] newEvents, EventBean[] oldEvents) {
        for (EventBean newEvent : newEvents) {
            // assess the toll for previous segment
            if ((byte)newEvent.get("oldSegment") != -1) {
                assessmentProcessor.assessToll((int)newEvent.get("vid"), (byte) newEvent.get("xway"), (short) newEvent.get("time"));
            }
            if ((int)newEvent.get("accSegment") == -1) { // no accident
                TollNotificationEvent tne = new TollNotificationEvent(newEvent);
                assessmentProcessor.rememberToll(tne.vid, (byte)newEvent.get("xway"), tne.toll);
                outputWriter.outputTollNotification(tne);
            } else { // accident!
                AccidentNotificationEvent ane = new AccidentNotificationEvent(newEvent);
                outputWriter.outputAccidentNotification(ane);
            }
        }
    }

}
