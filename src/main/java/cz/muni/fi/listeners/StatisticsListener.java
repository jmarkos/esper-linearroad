package cz.muni.fi.listeners;

import java.util.ArrayList;
import java.util.Collections;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;
import cz.muni.fi.eventtypes.InitialAverageSpeedEvent;

public class StatisticsListener implements UpdateListener {

    public String name;

    public StatisticsListener(String name) {
        this.name = name;
    }

    @Override
    public void update(EventBean[] newEvents, EventBean[] oldEvents) {
        ArrayList<InitialAverageSpeedEvent> s = new ArrayList<>(100);
        for (EventBean newEvent : newEvents) {
            s.add(new InitialAverageSpeedEvent(newEvent));
        }
        Collections.sort(s);
        System.out.println("Statistics listener " + name + ": " + newEvents.length + " here BE collection: " + s);
    }

}
