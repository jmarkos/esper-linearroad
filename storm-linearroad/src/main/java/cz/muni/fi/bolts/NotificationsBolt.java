package cz.muni.fi.bolts;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.espertech.esper.client.Configuration;
import com.espertech.esper.client.EPAdministrator;
import com.espertech.esper.client.EPRuntime;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.EPStatement;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;
import cz.muni.fi.TimeService;
import cz.muni.fi.eventtypes.AccidentEvent;
import cz.muni.fi.eventtypes.AccidentNotificationEvent;
import cz.muni.fi.eventtypes.ChangedSegmentEvent;
import cz.muni.fi.eventtypes.PositionReportEvent;
import cz.muni.fi.eventtypes.TollEvent;
import cz.muni.fi.eventtypes.TollNotificationEvent;
import cz.muni.fi.listeners.AverageSpeedListener;
import cz.muni.fi.listeners.CountListener;
import cz.muni.fi.listeners.InitialAverageSpeedListener;
import cz.muni.fi.listeners.NotificationListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NotificationsBolt extends BaseRichBolt {
    public Logger log = LoggerFactory.getLogger(NotificationsBolt.class);
    OutputCollector _collector;

    EPAdministrator cepAdm;
    EPRuntime cepRT;

    PrintWriter accidentNotificationWriter;
    PrintWriter tollNotificationWriter;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
        int taskId = context.getThisTaskId();
        String componentId = context.getThisComponentId();

        final TimeService timeService = new TimeService((Long) conf.get("BENCH_STARTTIME"));

        String outputDirectory = (String) conf.get("BENCH_OUTPUTDIRECTORY");
        String accidentNotificationsFileName = "accidentnotification" + taskId + ".txt";
        String tollNotificationsFileName = "tollnotifications" + taskId + ".txt";
        try {
            accidentNotificationWriter = new PrintWriter(new FileWriter(outputDirectory + accidentNotificationsFileName), true);
            tollNotificationWriter = new PrintWriter(new FileWriter(outputDirectory + tollNotificationsFileName), true);
        } catch (IOException e) {
            throw new RuntimeException("Creating file for output accident/toll notifications events failed. ", e);
        }

        Configuration cepConfig = new Configuration();
        cepConfig.addEventType("ChangedSegment", ChangedSegmentEvent.class.getName());
        cepConfig.addEventType("Toll", TollEvent.class.getName());
        EPServiceProvider cep = EPServiceProviderManager.getProvider(componentId + taskId, cepConfig);
        cepAdm = cep.getEPAdministrator();
        cepRT =  cep.getEPRuntime();

        EPStatement notifications = cepAdm.createEPL(
                "select CHS.vid as vid, CHS.time as time, CHS.xway as xway, CHS.newSegment as newSegment, CHS.oldSegment as oldSegment, T.averageSpeed as averageSpeed, T.toll as toll, T.accSegment as accSegment " +
                        "from ChangedSegment.win:time(5 sec) as CHS " +
                        "inner join Toll.win:time(90 sec) as T on CHS.min=T.min and CHS.xway=T.xway and CHS.direction=T.direction and CHS.newSegment=T.segment ");
        notifications.addListener(new UpdateListener() {
            @Override
            public void update(EventBean[] newEvents, EventBean[] oldEvents) {
                for (EventBean newEvent : newEvents) {
                    // ProcessTollEvent = byte oldSegment, int accSegment, int vid, short time, int toll
                    _collector.emit(new Values((Byte) newEvent.get("oldSegment"), (Integer) newEvent.get("accSegment"),
                            (Integer) newEvent.get("vid"), (Short) newEvent.get("time"), (Integer) newEvent.get("toll")));
                    // output notification/accident
                    if ((Integer)newEvent.get("accSegment") != -1) {
                        AccidentNotificationEvent accidentNotificationEvent = new AccidentNotificationEvent(newEvent);
                        accidentNotificationEvent.setOutputTime((short) timeService.getTime());
                        log.debug("Writing accident notification {}", accidentNotificationEvent);
//                        System.out.println("Writing accident notification " + accidentNotificationEvent);
                        accidentNotificationWriter.println(accidentNotificationEvent.toFileString());
                    } else {
                        TollNotificationEvent tollNotificationEvent = new TollNotificationEvent(newEvent);
                        tollNotificationEvent.setOutputTime((short) timeService.getTime());
                        log.debug("Writing toll notification {}", tollNotificationEvent);
//                        System.out.println("Writing toll notification " + tollNotificationEvent);
                        tollNotificationWriter.println(tollNotificationEvent.toFileString());
                    }
                }
            }
        });

        int xways = Integer.parseInt((String) conf.get("BENCH_XWAYS"));
        for (int i = 0; i < xways; i++) {
            for (int j = 0; j < 100; j++) {
                //int min, byte xway, byte direction, byte segment, double averageSpeed, long count, int accSegment, int toll
                cepRT.sendEvent(new TollEvent(0, (byte)i, (byte) 0, (byte)j, 0, 0, -1, 0));
                cepRT.sendEvent(new TollEvent(0, (byte)i, (byte) 1, (byte)j, 0, 0, -1, 0));
            }
        }
    }

    @Override
    public void execute(Tuple tuple) {
        String sourceComponent = tuple.getSourceComponent();
        if (sourceComponent.equals("changedSegmentBolt")) {
            // ChangedSegmentEvent = short time, int min, int vid, byte xway, byte lane, byte direction, byte oldSegment, byte newSegment
            ChangedSegmentEvent cse = new ChangedSegmentEvent();
            cse.time = tuple.getShort(0);
            cse.min = tuple.getInteger(1);
            cse.vid = tuple.getInteger(2);
            cse.xway = tuple.getByte(3);
            cse.lane = tuple.getByte(4);
            cse.direction = tuple.getByte(5);
            cse.oldSegment = tuple.getByte(6);
            cse.newSegment = tuple.getByte(7);
            cepRT.sendEvent(cse);
        } else { // toll
            // TollEvent = int min, byte xway, byte direction, byte segment, double averageSpeed, long count, int accSegment, int toll
            TollEvent te = new TollEvent();
            te.min = tuple.getInteger(0);
            te.xway = tuple.getByte(1);
            te.direction = tuple.getByte(2);
            te.segment = tuple.getByte(3);
            te.averageSpeed = tuple.getDouble(4);
            te.count = tuple.getLong(5);
            te.accSegment = tuple.getInteger(6);
            te.toll = tuple.getInteger(7);
            cepRT.sendEvent(te);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // ProcessToll event
        declarer.declare(new Fields("oldSegment", "accSegment", "vid", "time", "toll"));
    }

}
