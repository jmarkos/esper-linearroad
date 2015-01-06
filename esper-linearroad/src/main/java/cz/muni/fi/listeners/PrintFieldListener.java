package cz.muni.fi.listeners;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;

/**
 * Just a helper listener, useful for testing.
 */
public class PrintFieldListener implements UpdateListener {

    private final String field;

    public PrintFieldListener(String field) {
        this.field = field;
    }

    @Override
    public void update(EventBean[] newEvents, EventBean[] oldEvents) {
        System.out.print("update happening, new events: ");
        for (EventBean e : newEvents) {
            System.out.print(field + ": " + e.get(field) + ", ");
        }
        if (oldEvents != null) {
            System.out.print(" ,  old events: ");
            for (EventBean e : oldEvents) {
                System.out.print(e + ", ");
            }
        } else {
            System.out.print(" , no old events");
        }
        System.out.println();
    }
}
