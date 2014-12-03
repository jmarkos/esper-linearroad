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
            if ((int)newEvent.get("accSegment") == -1) { // no accident
                if ((byte)newEvent.get("oldSegment") == -1) {
                    // this was actually a CHS event with a car which entered the xway and didn't change segments
                    // just remember the toll, send notification
                    // this also solves the case when a car which left the xway has still a current toll, which will be overwritten
                    assessmentProcessor.rememberToll((int) newEvent.get("vid"), (int)newEvent.get("toll"));
                    TollNotificationEvent tne = new TollNotificationEvent(newEvent);
                    outputWriter.outputTollNotification(tne);
                } else {
                    // regular CHS, assess toll for previous segment, and save the current one
                    assessmentProcessor.assessToll((int) newEvent.get("vid"), (short) newEvent.get("time"));
                    assessmentProcessor.rememberToll((int)newEvent.get("vid"), (int)newEvent.get("toll"));
                    TollNotificationEvent tne = new TollNotificationEvent(newEvent);
                    outputWriter.outputTollNotification(tne);
                }
            } else { // accident!
                AccidentNotificationEvent ane = new AccidentNotificationEvent(newEvent);
                outputWriter.outputAccidentNotification(ane);
            }
        }
    }

}
