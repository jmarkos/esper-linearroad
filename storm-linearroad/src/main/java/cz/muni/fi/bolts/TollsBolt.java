package cz.muni.fi.bolts;

import java.util.ArrayList;
import java.util.HashSet;
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
import cz.muni.fi.Benchmark;
import cz.muni.fi.eventtypes.AccidentEvent;
import cz.muni.fi.eventtypes.AverageSpeedEvent;
import cz.muni.fi.eventtypes.ChangedSegmentEvent;
import cz.muni.fi.eventtypes.CountEvent;
import cz.muni.fi.eventtypes.InitialAverageSpeedEvent;
import cz.muni.fi.eventtypes.PositionReportEvent;
import cz.muni.fi.eventtypes.TollEvent;
import cz.muni.fi.listeners.AverageSpeedListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TollsBolt  extends BaseRichBolt {
    public Logger log = LoggerFactory.getLogger(TollsBolt.class);
    OutputCollector _collector;

    EPAdministrator cepAdm;
    EPRuntime cepRT;

    // each TollsBolt will process only a subset of the xways, and we don't know which one at compile time;
    // because of that, each instance will observe the PositionReports it is getting for the first minute of the
    // simulation, remembering the xways it has seen
    // we need to know the list of xways, because each minute we need segment statistics for all segments, even ones
    // which didn't have any traffic
    boolean findingXways = true;
    HashSet<Integer> foundXways;
    ArrayList<Integer> myXways;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
        int taskId = context.getThisTaskId();
        String componentId = context.getThisComponentId();

        foundXways = new HashSet<Integer>();
        myXways = new ArrayList<Integer>();

        Configuration cepConfig = new Configuration();
        cepConfig.addEventType("PositionReport", PositionReportEvent.class.getName());
        cepConfig.addEventType("InitialSpeed", InitialAverageSpeedEvent.class.getName());
        cepConfig.addEventType("Speed", AverageSpeedEvent.class.getName());
        cepConfig.addEventType("CountStats", CountEvent.class.getName());
        cepConfig.addEventType("Accident", AccidentEvent.class.getName());
        EPServiceProvider cep = EPServiceProviderManager.getProvider(componentId + taskId, cepConfig);
        cepAdm = cep.getEPAdministrator();
        cepRT =  cep.getEPRuntime();

        int xways = Integer.parseInt((String) conf.get("BENCH_XWAYS"));

        EPStatement initialSpeedStats = cepAdm.createEPL(
                "select Math.floor(coalesce(avg(time), -120)/60)+1 as min, xway, segment, direction, avg(speed) as averageSpeed " +
                        "from PositionReport.win:ext_timed_batch(time * 1000, 60 sec)  " +
                        "group by xway, segment, direction ");
        initialSpeedStats.addListener(new InitialAverageSpeedListener(cepRT, myXways, xways));

        EPStatement countStats = cepAdm.createEPL(
                "select Math.floor(coalesce(avg(time), -120)/60)+1 as min, xway, segment, direction, count(distinct vid) as count " +
                        "from PositionReport.win:ext_timed_batch(time * 1000, 60 sec)  " +
                        "group by xway, segment, direction ");
        countStats.addListener(new CountListener(cepRT, myXways, xways));

        EPStatement speedStats = cepAdm.createEPL(
                "select min, xway, segment, direction, avg(averageSpeed) as averageSpeed " +
                        "from InitialSpeed.std:groupwin(xway, segment, direction).win:length(5) "
                        +"group by xway, segment, direction ");
        speedStats.addListener(new AverageSpeedListener(cepRT));

        EPStatement tolls = cepAdm.createEPL(
                "select S.min as min, S.xway as xway, S.segment as segment, S.direction as direction, S.averageSpeed as averageSpeed, C.count as count, A.originalSegment as accSegment " +
                        "from Speed.win:time(90 sec) as S " +
                        "inner join CountStats.win:time(90 sec) as C on S.min=C.min and S.xway=C.xway and S.direction=C.direction and S.segment=C.segment " +
                        "left outer join Accident.win:time(90 sec).std:unique(min, xway, segment, direction) as A on A.min=S.min and A.xway=S.xway and A.direction=S.direction and A.segment=S.segment ");
        tolls.addListener(new UpdateListener() {
            @Override
            public void update(EventBean[] newEvents, EventBean[] oldEvents) {
                for (EventBean newEvent : newEvents) {
                    TollEvent tollEvent = new TollEvent(newEvent);
                    log.debug("Sending toll {}", tollEvent);
//                    System.out.println("Sending toll " + tollEvent);
                    // TollsEvent = int min, byte xway, byte direction, byte segment, double averageSpeed, long count, int accSegment, int toll
                    _collector.emit("toll", new Values(tollEvent.min, tollEvent.xway, tollEvent.direction, tollEvent.segment, tollEvent.averageSpeed, tollEvent.count, tollEvent.accSegment, tollEvent.toll));
                }
            }
        });
    }

    @Override
    public void execute(Tuple tuple) {
        // this bolt is getting PositionReportEvents and AccidentEvents
        // PositionReportEvent = byte type, short time, int vid, byte speed, byte xway, byte lane, byte direction, byte segment, int position
        // AccidentEvent = int min, byte xway, byte direction, byte segment, byte originalSegment, int position
        if (tuple.getSourceComponent().equals("accidentDetectionBolt")) { // Accident
            AccidentEvent ae = new AccidentEvent();
            ae.min = tuple.getInteger(0);
            ae.xway = tuple.getByte(1);
            ae.direction = tuple.getByte(2);
            ae.segment = tuple.getByte(3);
            ae.originalSegment = tuple.getByte(4);
            ae.position = tuple.getInteger(5);
            cepRT.sendEvent(ae);
        } else { // PR
            PositionReportEvent pre = new PositionReportEvent();
            pre.type = tuple.getByte(0);
            pre.time = tuple.getShort(1);
            pre.vid = tuple.getInteger(2);
            pre.speed = tuple.getByte(3);
            pre.xway = tuple.getByte(4);
            pre.lane = tuple.getByte(5);
            pre.direction = tuple.getByte(6);
            pre.segment = tuple.getByte(7);
            pre.position = tuple.getInteger(8);
            if (findingXways) {
                foundXways.add((int) pre.xway);
                if (pre.time > 50) {
                    // finish the lookup phase
                    findingXways = false;
                    for (Integer foundXway : foundXways) {
                        myXways.add(foundXway);
                    }
                }
            }
            cepRT.sendEvent(pre);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // TollEvent = int min, byte xway, byte direction, byte segment, double averageSpeed, long count, int accSegment, int toll
        declarer.declareStream("toll", new Fields("min", "xway", "direction", "segment", "averageSpeed", "count", "accSegment", "toll"));
    }

    class InitialAverageSpeedListener implements UpdateListener {

        public Logger log = LoggerFactory.getLogger(InitialAverageSpeedListener.class);
        private final EPRuntime cepRT;
        private ArrayList<Integer> myXways;
        private int xways;

        public InitialAverageSpeedListener(EPRuntime cepRT, ArrayList<Integer> myXways, int xways) {
            this.cepRT = cepRT;
            this.myXways = myXways;
            this.xways = xways;
        }

        @Override
        public void update(EventBean[] newEvents, EventBean[] oldEvents) {
            log.info("Number of active segments: " + newEvents.length);
//            System.out.println("Number of active segments: " + newEvents.length);
            // we need to list through and find all missing segments (segments which didn't have any action), and produce
            // an 'empty' statistic event for them, so that the downstream query can use length(5)

            // east=0, west=1, first 100 is east, 100-200 is west
            boolean[][] existingStats = new boolean[xways][200];
            int min = 0;
            for (EventBean newEvent : newEvents) {
                if (((Double)newEvent.get("min")).intValue() == -1) {
                    continue;
                    // sometimes esper generates "empty" events, i.e. with min=null, averageSpeed=null but with actual xway,seg,dir set,
                    // so we just skip those and create them properly afterwards (with min and averageSpeed != null)
                }
                InitialAverageSpeedEvent averageSpeedEvent = new InitialAverageSpeedEvent(newEvent);
                min = averageSpeedEvent.min;
                existingStats[averageSpeedEvent.xway][averageSpeedEvent.segment + averageSpeedEvent.direction*100] = true;
                log.debug("Sending initial average speed {}", averageSpeedEvent);
//                System.out.println("Sending initial average speed " + averageSpeedEvent);
                cepRT.sendEvent(averageSpeedEvent);
            }
            int numEmpty = 0;

            for (Integer myXway : myXways) {
                // east
                for (int j = 0; j < 100; j++) {
                    if (! existingStats[myXway][j]) {
                        InitialAverageSpeedEvent ias = new InitialAverageSpeedEvent(min, myXway, 0, j, 0);
                        log.debug("Sending empty initial average speed {}", ias);
//                        System.out.println("Sending empty initial average speed " + ias);
                        cepRT.sendEvent(ias);
                        numEmpty++;
                    }
                }
                // west
                for (int j = 100; j < 200; j++) {
                    if (! existingStats[myXway][j]) {
                        InitialAverageSpeedEvent ias = new InitialAverageSpeedEvent(min, myXway, 1, j - 100, 0);
                        log.debug("Sending empty initial average speed {}", ias);
//                        System.out.println("Sending empty initial average speed " + ias);
                        cepRT.sendEvent(ias);
                        numEmpty++;
                    }
                }
            }
            log.info("Sent " + numEmpty + " empty initial average speed events.");
//            System.out.println("Sent " + numEmpty + " empty initial average speed events.");
        }

    }

    class CountListener implements UpdateListener {

        public Logger log = LoggerFactory.getLogger(CountListener.class);
        private final EPRuntime cepRT;
        private ArrayList<Integer> myXways;
        private int xways;

        public CountListener(EPRuntime cepRT, ArrayList<Integer> myXways, int xways) {
            this.cepRT = cepRT;
            this.myXways = myXways;
            this.xways = xways;
        }

        @Override
        public void update(EventBean[] newEvents, EventBean[] oldEvents) {
            log.info("Number of active segments: " + newEvents.length);
//            System.out.println("Number of active segments: " + newEvents.length);
            // we need to list through and find all missing segments (segments which didn't have any action), and produce
            // an 'empty' statistic event for them, so that we can later do an inner join

            // east=0, west=1, first 100 is east, 100-200 is west
            boolean[][] existingStats = new boolean[xways][200];
            int min = 0;
            for (EventBean newEvent : newEvents) {
                if (((Double)newEvent.get("min")).intValue() == -1) {
                    continue;
                    // sometimes esper generates "empty" events, i.e. with min=null, count=0 but with actual xway,seg,dir set,
                    // so we just skip those and create them properly afterwards (with min != null)
                    // TODO find out the real cause / report a bug
                }
                CountEvent countEvent = new CountEvent(newEvent);
                min = countEvent.min;
                existingStats[countEvent.xway][countEvent.segment + countEvent.direction*100] = true;
                log.debug("Sending count {}", countEvent);
//                System.out.println("Sending count " + countEvent);
                cepRT.sendEvent(countEvent);
            }
            int numEmpty = 0;
            for (Integer myXway : myXways) {
                // east
                for (int j = 0; j < 100; j++) {
                    if (! existingStats[myXway][j]) {
                        CountEvent countEvent = new CountEvent(min, myXway, 0, j, 0);
                        log.debug("Sending empty count {}", countEvent);
//                        System.out.println("Sending empty count " + countEvent);
                        cepRT.sendEvent(countEvent);
                        numEmpty++;
                    }
                }
                // west
                for (int j = 100; j < 200; j++) {
                    if (! existingStats[myXway][j]) {
                        CountEvent countEvent = new CountEvent(min, myXway, 1, j - 100, 0);
                        log.debug("Sending empty count {}", countEvent);
//                        System.out.println("Sending empty count " + countEvent);
                        cepRT.sendEvent(countEvent);
                        numEmpty++;
                    }
                }
            }
            log.info("Sent " + numEmpty + " empty count events.");
//            System.out.println("Sent " + numEmpty + " empty count events.");
        }

    }
}
