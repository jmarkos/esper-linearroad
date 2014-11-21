package cz.muni.fi;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;

import com.espertech.esper.client.Configuration;
import com.espertech.esper.client.EPAdministrator;
import com.espertech.esper.client.EPRuntime;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.EPStatement;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;
import com.espertech.esper.client.time.CurrentTimeEvent;
import cz.muni.fi.eventtypes.*;
import cz.muni.fi.listeners.*;


/*
The point is to make it as simple as possible, to allow for maximum decomposition and subsequent distributing

Esper can leverage multiple threads in 2 ways:
1. setting it's own internal inbound thread pool - the ordering guarantees are lost, is that a problem?
   stoppedCars -> no
   trashedCars -> no, because the reports of a single car are 30s apart (to mess the ordering there would need to be a 30s 'lag'
                  in which case ordering is the least of our problems)
   segmentStats -> yes if we use ext_timed window for the stats, because then some future event can end the window before prematurely
                    (for example event with time 60 comes before all of the events from time 59 are processed - can easily happen)
   others - TODO

2. sending events from multiple threads
 */
public class Benchmark {

    DataDriver datadriver;
    DailyExpenditureProcessor dailyExpenditureProcessor;

    public static void main(String[] args) throws InterruptedException, IOException {
        Benchmark b = new Benchmark();
        b.run();
    }

    public void run() throws InterruptedException, IOException {
        Configuration cepConfig = new Configuration();
        cepConfig.addEventType("LRB", LRBEvent.class.getName());

        cepConfig.addEventType("PositionReport", PositionReportEvent.class.getName());
        cepConfig.addEventType("ChangedSegment", ChangedSegmentEvent.class.getName());
        cepConfig.addEventType("StoppedCar", StoppedCarEvent.class.getName());

        cepConfig.addEventType("TrashedCar", TrashedCarEvent.class.getName());
        cepConfig.addEventType("Accident", AccidentEvent.class.getName());

        cepConfig.getEngineDefaults().getThreading().setInternalTimerEnabled(false);
        cepConfig.getEngineDefaults().getThreading().setThreadPoolInbound(true);
        cepConfig.getEngineDefaults().getThreading().setThreadPoolInboundNumThreads(4);

        EPServiceProvider cep = EPServiceProviderManager.getProvider(null, cepConfig);
        EPAdministrator cepAdm = cep.getEPAdministrator();

        final EPRuntime cepRT =  cep.getEPRuntime();

        // just so segment statistics are aligned properly with simulation time, i.e. they fire at second 59 of each minute
        // it's still hard to keep things 100% accurate, we might send the new time event when not all previously sent events are processed
        // TODO try to use real time for the realtime simulation (maybe start the datadriver at second 1 or 2)
        cepRT.sendEvent(new CurrentTimeEvent(-1000));

        EPStatement countQuery = cepAdm.createEPL(
                "select count(*) as cnt from LRB.win:time_batch(30 sec)");
//        countQuery.addListener(new listeners.PrintFieldListener("cnt"));

        EPStatement averageSpeedStats = cepAdm.createEPL(
                "insert into averageSpeedStats select xway, segment, direction, avg(speed) as averageSpeed " +
                        "from LRB(type=0).win:time(5 min) " +
                        "group by xway, segment, direction " +
                        "output snapshot every 60 seconds");

        EPStatement countStats = cepAdm.createEPL(
                "insert into countStats select xway, segment, direction, count(distinct vid) as count " +
                        "from LRB(type=0).win:time_batch(60 sec) " +
                        "group by xway, segment, direction ");

        averageSpeedStats.addListener(new StatisticsListener("ALL"));
        countStats.addListener(new UpdateListener() {
            @Override
            public void update(EventBean[] newEvents, EventBean[] oldEvents) {
                ArrayList<CountStats> s = new ArrayList<>(100);
                for (EventBean newEvent : newEvents) {
                    s.add(new CountStats(newEvent));
                }
                Collections.sort(s);
                System.out.println("Count Listener: " + newEvents.length + " here BE collection: " + s);
            }
        });

//        // speed=0 implies type=0
//        EPStatement stoppedCarFilter = cepAdm.createEPL(
//                "insert into stoppedCars select * from LRB as la where la.speed=0");

        EPStatement trashedCars = cepAdm.createPattern(
                  " (every pr0=StoppedCar) " +
                          "-> pr1=StoppedCar(vid=pr0.vid) where timer:within(35 sec) " +
                          "-> pr2=StoppedCar(vid=pr0.vid) where timer:within(35 sec) " +
                          "-> pr3=StoppedCar(vid=pr0.vid) where timer:within(35 sec)");
        trashedCars.addListener(new TrashedCarListener(cepRT));

        // distinct filters multiple-car accidents, because a new car matches with all cars in the accident
        // we output the accidents each minute (PR from minute t should take into account accidents from minute t-1),
        // and multiply them in the listener for all the affected segments
        EPStatement accidents = cepAdm.createEPL(
                "select distinct t1.xway as xway, t1.segment as segment, t1.direction as direction, t1.position as position " +
                "from TrashedCar.win:time(30 sec) as t1 " +
                "inner join TrashedCar.win:length(1) as t2 on t1.xway=t2.xway and t1.segment=t2.segment and t1.direction=t2.direction and t1.position=t2.position " +
                "where t1.vid != t2.vid " +
                "output every 60 seconds");
        accidents.addListener(new AccidentListener(cepRT));


        // TODO the unique is needed to consider only the newest accidents
//        EPStatement accidentsProcess = cepAdm.createEPL(
//                "select * from accidents.win:time(30 sec).std:unique(xway, segment, direction)");

        // stream of 'new' cars - position reports which changed the segment of a car
        // this works like this: the 35s window has 2 consecutive reports for each car, the current one (=latest one) and the previous
        // the 2nd window keeps just the latest one and compares it only with the previous (the where condition fails when compared with itself)
        EPStatement changedSegment = cepAdm.createEPL(
                "select pr2.time as time, pr2.vid as vid, pr1.segment as oldSegment, pr2.segment as newSegment, pr2.xway as xway, pr2.direction as direction, pr2.lane as lane " +
                "from PositionReport.win:time(35 sec) as pr1 " +
                "inner join PositionReport(type=0).win:length(1) as pr2 on pr1.vid=pr2.vid " +
                "where pr2.segment!=pr1.segment ");
        changedSegment.addListener(new ChangedSegmentListener(cepRT));

       testChangedSegment(cepRT);

        // TODO generovat tolls pre cely segment? potom joinovat kazde vid iba s tym?
        // TODO na stats treba aj time window, ak v novej minute neboli ziadne auta tak budeme mat stare stats
        // we have a >minute window on the stats, so we get 2 matches, we choose only the recent one
        EPStatement assessTolls = cepAdm.createEPL(
                "@Audit select TC.vid as vid, AST.averageSpeed as averageSpeed, CT.count as count, ACC.segment as accident " +
//                        "from tollCars.std:lastevent() as TC " +
                        "from tollCars as TC unidirectional " +
                        "left outer join averageSpeedStats.std:unique(xway, segment, direction) as AST on TC.xway=AST.xway and TC.newSegment=AST.segment and TC.direction=AST.direction " +
                        "left outer join countStats.std:unique(xway, segment, direction) as CT on TC.xway=CT.xway and TC.newSegment=CT.segment and TC.direction=CT.direction "
                       + "left outer join accidents.win:time(30 sec).std:unique(xway, segment, direction) as ACC on TC.xway=ACC.xway and TC.direction=ACC.direction " +
                        "where (not exists(ACC.segment)) or (ACC.direction=0 and ACC.segment - TC.newSegment <= 4 and ACC.segment - TC.newSegment >= 0) or " +
                              "(ACC.direction=1 and TC.newSegment - ACC.segment <= 4 and TC.newSegment - ACC.segment >= 0)"
                // TODO toto je zle: join podla x, d, ale unique je na x, s, d, takze vzdy dostaneme iba jeden accident
                // TODO dalej ta where podmienka sa vyhodnoti iba pre ten jeden accident ktory sa joinol
        );
        assessTolls.addListener(new UpdateListener() {
            @Override
            public void update(EventBean[] newEvents, EventBean[] oldEvents) {
                for (EventBean newEvent : newEvents) {
                    System.out.println("car " + newEvent.get("vid") + " speed " + newEvent.get("averageSpeed") + " count " + newEvent.get("count") + " accident: " + newEvent.get("accident"));
                }
            }
        });

        cepRT.sendEvent(new CurrentTimeEvent(0));
        cepRT.sendEvent(createSegEvent(1, 1, 10, 0));
        cepRT.sendEvent(createSegEvent(2, 2, 10, 99));
        cepRT.sendEvent(createSegEvent(3, 3, 0, 2));
        cepRT.sendEvent(createSegEvent(4, 3, 0, 2));
        cepRT.sendEvent(createSegEvent(5, 3, 0, 2));
        cepRT.sendEvent(createSegEvent(6, 3, 0, 2));
        cepRT.sendEvent(createSegEvent(3, 4, 0, 2));
        cepRT.sendEvent(createSegEvent(4, 4, 0, 2));
        cepRT.sendEvent(createSegEvent(5, 4, 0, 2));
        cepRT.sendEvent(createSegEvent(6, 4, 0, 2));
        cepRT.sendEvent(createSegEvent(3, 3, 21, 98));
        cepRT.sendEvent(createSegEvent(3, 3, 33, 98));
        cepRT.sendEvent(new CurrentTimeEvent(32000));
        cepRT.sendEvent(createSegEvent(30, 1, 10, 1));
        cepRT.sendEvent(createSegEvent(32, 2, 10, 99));
        Thread.sleep(500);
        cepRT.sendEvent(new CurrentTimeEvent(62000));
        cepRT.sendEvent(createSegEvent(60, 1, 10, 1));
        cepRT.sendEvent(createSegEvent(62, 2, 10, 99));
        cepRT.sendEvent(new CurrentTimeEvent(92000));
        cepRT.sendEvent(createSegEvent(90, 1, 10, 2));
        cepRT.sendEvent(createSegEvent(92, 2, 10, 98));
        Thread.sleep(3000);
        System.exit(0);


        try {
//            datadriver = new DataDriver("/home/van/dipl/lroad_data/0.5/cardatapoints.out");
            datadriver = new DataDriver("/home/van/dipl/parallel-esper/esper-lrb/data/datafile3hours.dat");
            datadriver.setSpeedup(1);
            datadriver.start();
        } catch (IOException e) {
            e.printStackTrace();
        }

        // TODO potrebujem spracovat tie data z L=5, treba tam prehodit xway

        dailyExpenditureProcessor = new DailyExpenditureProcessor("/home/van/dipl/linearRoad/input-downloaded/histtolls.txt");



//        cepRT.sendEvent(new CurrentTimeEvent(0));
//        cepRT.sendEvent(createSegEvent(1, 1, 10, 0));
//        cepRT.sendEvent(createSegEvent(2, 1, 10, 0));
//        cepRT.sendEvent(createSegEvent(1, 3, 10, 0));
//        cepRT.sendEvent(createSegEvent(1, 4, 10, 0));
//        cepRT.sendEvent(createSegEvent(2, 2, 10, 99));
//        cepRT.sendEvent(new CurrentTimeEvent(13000));
//        cepRT.sendEvent(createSegEvent(30, 1, 10, 1));
//        cepRT.sendEvent(createSegEvent(32, 2, 10, 99));
//        cepRT.sendEvent(new CurrentTimeEvent(21000));
//        Thread.sleep(3000);
//        System.exit(0);

//        cepRT.sendEvent(new CurrentTimeEvent(0));
//        cepRT.sendEvent(createEvent(1, 1, 10));
//        cepRT.sendEvent(createEvent(1, 2, 10));
//        cepRT.sendEvent(createEvent(1, 3, 10));
//        cepRT.sendEvent(createEvent(1, 4, 10));
//        System.out.println("hybeme cas dalej");
//        cepRT.sendEvent(new CurrentTimeEvent(60000));
//        cepRT.sendEvent(new CurrentTimeEvent(65000));
//        cepRT.sendEvent(new CurrentTimeEvent(70000));
//        cepRT.sendEvent(new CurrentTimeEvent(75000));
//        cepRT.sendEvent(new CurrentTimeEvent(110000));
//        cepRT.sendEvent(createEvent(1, 5, 10));
//        cepRT.sendEvent(createEvent(1, 6, 15));
//        cepRT.sendEvent(createEvent(1, 7, 20));
//        System.out.println("hybeme sa daleeeej");
//        cepRT.sendEvent(new CurrentTimeEvent(120000));
//        cepRT.sendEvent(new CurrentTimeEvent(125000));
//        cepRT.sendEvent(new CurrentTimeEvent(185000));
//        cepRT.sendEvent(new CurrentTimeEvent(245000));
//        cepRT.sendEvent(new CurrentTimeEvent(305000));
//        cepRT.sendEvent(new CurrentTimeEvent(365000));
//


//        cepRT.sendEvent(createEvent(1, 1, 0));
//        cepRT.sendEvent(createEvent(1, 1, 0));
//        cepRT.sendEvent(createEvent(1, 1, 0));
//        cepRT.sendEvent(createEvent(1, 1, 0));

//        cepRT.sendEvent(createEvent(1, 2, 0));
//        cepRT.sendEvent(createEvent(1, 2, 0));
//        cepRT.sendEvent(createEvent(1, 2, 0));
//        cepRT.sendEvent(createEvent(1, 2, 0));
//
//        cepRT.sendEvent(createEvent(1, 3, 0));
//        cepRT.sendEvent(createEvent(1, 3, 0));
//        cepRT.sendEvent(createEvent(1, 3, 0));
//        cepRT.sendEvent(createEvent(1, 3, 0));
//
//        cepRT.sendEvent(createEvent(1, 4, 0));
//        cepRT.sendEvent(createEvent(1, 4, 0));
//        cepRT.sendEvent(createEvent(1, 4, 0));
//        cepRT.sendEvent(createEvent(1, 4, 0));
//
//        cepRT.sendEvent(createEvent(1, 5, 0));
//        cepRT.sendEvent(createEvent(1, 5, 0));
//        cepRT.sendEvent(createEvent(1, 5, 0));
//        cepRT.sendEvent(createEvent(1, 5, 0));
//        System.out.println("Sleeping " + 5000 + " milliseconds");
//
//        System.out.println("accProcess = " + accProcess[0]);
//        Thread.sleep(35000);
//        System.exit(0);

        int sum = 0;
        while (!datadriver.simulationEnded) {
            ArrayDeque<Event> newEvents = datadriver.getNewEvents();
            if (newEvents != null) {
                long time = newEvents.getFirst().getTime() * 1000;
                cepRT.sendEvent(new CurrentTimeEvent(time));
                sum += newEvents.size();
                System.out.println("sum = " + sum + ", sec: " + newEvents.getFirst().getTime());
                for (Event newEvent : newEvents) {
                    if (newEvent.getType() == 3) {
                        dailyExpenditureProcessor.handleQuery((DailyExpenditureQuery) newEvent);
                    }
                    if (newEvent.getType() == 2) {
                        // TODO call to assessments agent
                    }
                    if (newEvent.getType() == 0) {
                        PositionReportEvent pre = (PositionReportEvent) newEvent;
                        if (pre.speed == 0) {
                            cepRT.sendEvent(new StoppedCarEvent(pre));
                        }
                        if (pre.lane == 0) {
                            // entrance lane, we need to calculate the toll
                            cepRT.sendEvent(new ChangedSegmentEvent(pre));
                        }
                        // send the PositionReport for the statistics and further ChangedSegmentEvent detection
                        cepRT.sendEvent(pre);
                    }
                    cepRT.sendEvent(newEvent);
                }
            }
            Thread.sleep(10); // keep it low so we don't waste i.e. 90ms just because of checking 10ms before next second
        }
        System.out.println("sum = " + sum);
        Thread.sleep(30000);
    }

    public static LRBEvent createEvent(int time, int vid, int speed) {
        LRBEvent result = new LRBEvent();
        result.time = (short) time;
        result.vid = vid;
        result.speed = (byte) speed;
        return result;
    }

    public static void sendEventWithTime(EPRuntime cepRT, int time, int vid, int speed) throws InterruptedException {
        cepRT.sendEvent(createEvent(time, vid, speed));
        Thread.sleep(500);
        sendTime(cepRT, (time + 1) * 1000);
    }

    public static LRBEvent createSegEvent(int time, int vid, int speed, int segment) {
        LRBEvent result = new LRBEvent();
        result.time = (short) time;
        result.vid = vid;
        result.speed = (byte) speed;
        result.segment = (byte) segment;
        return result;
    }

    // turn off esper's internal threading before running this
    public static void testChangedSegment(EPRuntime cepRT) throws InterruptedException {
        cepRT.sendEvent(new CurrentTimeEvent(0));
        cepRT.sendEvent(new PositionReportEvent((short) 1, 1, (byte)0));
        cepRT.sendEvent(new PositionReportEvent((short) 2, 2, (byte)99));
        cepRT.sendEvent(new CurrentTimeEvent(32000));
        cepRT.sendEvent(new PositionReportEvent((short) 30, 1, (byte)1));
        cepRT.sendEvent(new PositionReportEvent((short) 32, 2, (byte)99));
        cepRT.sendEvent(new CurrentTimeEvent(62000));
        cepRT.sendEvent(new PositionReportEvent((short) 60, 1, (byte)1));
        cepRT.sendEvent(new PositionReportEvent((short) 62, 2, (byte)99));
        cepRT.sendEvent(new CurrentTimeEvent(92000));
        cepRT.sendEvent(new PositionReportEvent((short) 90, 1, (byte)2));
        cepRT.sendEvent(new PositionReportEvent((short) 92, 2, (byte)98));
        Thread.sleep(3000);
        System.exit(0);
    }

    public static void testAccidents(EPRuntime cepRT) throws InterruptedException {
        cepRT.sendEvent(new CurrentTimeEvent(0));
        // short time, int vid, byte xway, byte lane, byte direction, byte segment
        cepRT.sendEvent(new TrashedCarEvent((short)1, 101, (byte)0, (byte)0, (byte)0, (byte)0, 2333));
        cepRT.sendEvent(new TrashedCarEvent((short)1, 102, (byte)0, (byte)0, (byte)0, (byte)0, 2333));
        cepRT.sendEvent(new TrashedCarEvent((short)1, 103, (byte)0, (byte)0, (byte)0, (byte)0, 2333));
        cepRT.sendEvent(new TrashedCarEvent((short)1, 104, (byte)0, (byte)0, (byte)0, (byte)0, 2334));
        cepRT.sendEvent(new TrashedCarEvent((short)1, 105, (byte)0, (byte)0, (byte)0, (byte)3, 2334));
        cepRT.sendEvent(new TrashedCarEvent((short)1, 111, (byte)0, (byte)0, (byte)0, (byte)5, 883));
        cepRT.sendEvent(new TrashedCarEvent((short)1, 112, (byte)0, (byte)0, (byte)0, (byte)5, 883));
        cepRT.sendEvent(new CurrentTimeEvent(65000));
        cepRT.sendEvent(new CurrentTimeEvent(150000));

        System.out.println("Sleeping " + 3000 + " milliseconds");
        Thread.sleep(3000);
        System.exit(0);
    }

    public static void testTrashedCars(EPRuntime cepRT) throws InterruptedException {
        sendEventWithTime(cepRT, 30, 1, 0);
        sendEventWithTime(cepRT, 60, 1, 0);
        sendEventWithTime(cepRT, 90, 1, 0);
        sendEventWithTime(cepRT, 120, 1, 1);
        sendEventWithTime(cepRT, 150, 1, 0);
        sendEventWithTime(cepRT, 180, 1, 3);
        sendEventWithTime(cepRT, 210, 1, 4);
        sendEventWithTime(cepRT, 240, 1, 0);
        sendEventWithTime(cepRT, 270, 1, 0);
        sendEventWithTime(cepRT, 300, 1, 1);
        sendEventWithTime(cepRT, 330, 1, 0);
        sendEventWithTime(cepRT, 360, 1, 0);
        sendEventWithTime(cepRT, 390, 1, 0);
        sendEventWithTime(cepRT, 420, 1, 3);
        sendTime(cepRT, 450000);
        Thread.sleep(3000);
        System.exit(0);
    }

    public static void sendTime(EPRuntime cepRT, long millis) {
        cepRT.sendEvent(new CurrentTimeEvent(millis));
        System.out.println("send time: " + millis);
    }

}
