package cz.muni.fi;

import java.io.IOException;
import java.util.ArrayDeque;

import com.espertech.esper.client.Configuration;
import com.espertech.esper.client.EPAdministrator;
import com.espertech.esper.client.EPRuntime;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.EPStatement;
import com.espertech.esper.client.time.CurrentTimeEvent;
import cz.muni.fi.eventtypes.*;
import cz.muni.fi.listeners.*;
import org.apache.log4j.Logger;

/*
Esper can leverage multiple threads in 2 ways:
1. setting it's own internal inbound thread pool - the ordering guarantees are lost, is that a problem?
   TODO (atm using single thread)

2. sending events from multiple threads
 */
public class Benchmark {

    private static org.apache.log4j.Logger log = Logger.getLogger(Benchmark.class);

    DataDriver datadriver;
    public static int NUM_XWAYS = 5;

    public static void main(String[] args) throws InterruptedException, IOException {
        Benchmark b = new Benchmark();
        b.run();
    }

    public void run() throws InterruptedException, IOException {
        Configuration cepConfig = new Configuration();
        cepConfig.addEventType("LRB", LRBEvent.class.getName()); // TODO not used

        cepConfig.addEventType("PositionReport", PositionReportEvent.class.getName());
        cepConfig.addEventType("ChangedSegment", ChangedSegmentEvent.class.getName());

        cepConfig.addEventType("InitialSpeed", InitialAverageSpeedEvent.class.getName());
        cepConfig.addEventType("Speed", AverageSpeedEvent.class.getName());
        cepConfig.addEventType("CountStats", CountEvent.class.getName());
        cepConfig.addEventType("Toll", TollEvent.class.getName());

        cepConfig.addEventType("StoppedCar", StoppedCarEvent.class.getName());
        cepConfig.addEventType("TrashedCar", TrashedCarEvent.class.getName());
        cepConfig.addEventType("Accident", AccidentEvent.class.getName());

        cepConfig.getEngineDefaults().getThreading().setInternalTimerEnabled(false);
//        cepConfig.getEngineDefaults().getThreading().setThreadPoolInbound(true);
//        cepConfig.getEngineDefaults().getThreading().setThreadPoolInboundNumThreads(4);


        EPServiceProvider cep = EPServiceProviderManager.getProvider(null, cepConfig);
        EPAdministrator cepAdm = cep.getEPAdministrator();

//        datadriver = new DataDriver("/home/van/dipl/parallel-esper/esper-lrb/data/datafile3hours.dat");
        datadriver = new DataDriver("/home/van/dipl/lroad_data/5/merged5.out");
        datadriver.setSpeedup(1);

        final EPRuntime cepRT =  cep.getEPRuntime();
        final OutputWriter outputWriter = new OutputWriterImpl(datadriver);
        final AssessmentProcessor assessmentProcessor = new AssessmentProcessor();
        final DailyExpenditureProcessor dailyExpenditureProcessor = new DailyExpenditureProcessor(null, outputWriter, "jdbc:postgresql://localhost/lrb");

        // reports every 60 seconds of position reports as [minute, x, s, d, averageSpeed]
        EPStatement initialSpeedStats = cepAdm.createEPL(
                "select Math.floor(coalesce(avg(time), -120)/60)+1 as min, xway, segment, direction, avg(speed) as averageSpeed " +
                        "from PositionReport.win:ext_timed_batch(time * 1000, 60 sec)  " +
                        "group by xway, segment, direction ");
        initialSpeedStats.addListener(new InitialAverageSpeedListener(cepRT));

        // reports every 60 seconds of position reports as [minute, x, s, d, count]
        EPStatement countStats = cepAdm.createEPL(
                "select Math.floor(coalesce(avg(time), -120)/60)+1 as min, xway, segment, direction, count(distinct vid) as count " +
                        "from PositionReport.win:ext_timed_batch(time * 1000, 60 sec)  " +
                        "group by xway, segment, direction ");
        countStats.addListener(new CountListener(cepRT));

        // computes average speed over last 5 minutes, by using last 5 minute averages
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
        tolls.addListener(new TollListener(cepRT));

        // 5 second window on new cars - if it takes longer to compute the statistics, it's too late anyway
        // can't do left outer join, because then the earlier CHS events won't 'wait' for their stats
        EPStatement notifications = cepAdm.createEPL(
                "select CHS.vid as vid, CHS.time as time, CHS.xway as xway, CHS.newSegment as newSegment, CHS.oldSegment as oldSegment, T.averageSpeed as averageSpeed, T.toll as toll, T.accSegment as accSegment " +
                        "from ChangedSegment.win:time(5 sec) as CHS " +
                        "inner join Toll.win:time(90 sec) as T on CHS.min=T.min and CHS.xway=T.xway and CHS.direction=T.direction and CHS.newSegment=T.segment ");
        notifications.addListener(new NotificationListener(cepRT, outputWriter, assessmentProcessor));

        // even if a car sends 4 reports with speed 0, it's position may change => not an accident
        // TODO need to join on x,s,d, too, because a car can travel on multiple xways at the same time
        EPStatement trashedCars = cepAdm.createPattern(
                  " (every pr0=StoppedCar) " +
                          "-> pr1=StoppedCar(vid=pr0.vid and position=pr0.position and xway=pr0.xway) where timer:within(35 sec) " +
                          "-> pr2=StoppedCar(vid=pr0.vid and position=pr0.position and xway=pr0.xway) where timer:within(35 sec) " +
                          "-> pr3=StoppedCar(vid=pr0.vid and position=pr0.position and xway=pr0.xway) where timer:within(35 sec)");
        trashedCars.addListener(new TrashedCarListener(cepRT));

        // distinct filters multiple-car accidents, because a new car matches with all cars in the accident
        // every accident is copied for all segments it affects in the listener
        // TODO isn't it enough to join on position? what about direction?
        EPStatement accidents = cepAdm.createEPL(
                "select distinct Math.floor(t2.time/60)+1 as min, t1.xway as xway, t1.segment as segment, t1.direction as direction, t1.position as position " +
                "from TrashedCar.win:time(30 sec) as t1 " +
                "inner join TrashedCar.win:length(1) as t2 on t1.xway=t2.xway and t1.segment=t2.segment and t1.direction=t2.direction and t1.position=t2.position " +
                "where t1.vid != t2.vid "
        );
        accidents.addListener(new AccidentListener(cepRT));

        // stream of 'new' cars - position reports which changed the segment of a car
        // this works like this: the 35s window has 2 consecutive reports for each car, the current one (=latest one) and the previous
        // the 2nd window keeps just the latest one and compares it only with the previous (the where condition fails when compared with itself) TODO make this more clear
        // TODO we need to join on x,s,d also, because the car can travel on multiple xways simultaneously...
        EPStatement changedSegment = cepAdm.createEPL(
                "select pr2.time as time, pr2.vid as vid, pr1.segment as oldSegment, pr2.segment as newSegment, pr2.xway as xway, pr2.direction as direction, pr2.lane as lane " +
                "from PositionReport.win:time(35 sec) as pr1 " +
                "inner join PositionReport.win:length(1) as pr2 on pr1.vid=pr2.vid and pr1.xway=pr2.xway " +
                "where pr2.segment!=pr1.segment ");
        changedSegment.addListener(new ChangedSegmentListener(cepRT));

        datadriver.start();

        // need to send empty tolls for the first minute, so that the first cars have something to join on
        for (int i = 0; i < Benchmark.NUM_XWAYS; i++) {
            for (int j = 0; j < 100; j++) {
                //int min, byte xway, byte direction, byte segment, double averageSpeed, long count, int accSegment, int toll
                cepRT.sendEvent(new TollEvent(0, (byte)i, (byte) 0, (byte)j, 0, 0, -1, 0));
                cepRT.sendEvent(new TollEvent(0, (byte)i, (byte) 1, (byte)j, 0, 0, -1, 0));
            }
        }

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
                        AccountBalanceQuery abq = (AccountBalanceQuery) newEvent;
                        AssessmentProcessor.Balance balance = assessmentProcessor.getBalance(abq.getVid());
                        AccountBalanceResponse abr = new AccountBalanceResponse(abq.getTime(), abq.getQid(), balance.balance, balance.lastUpdated);
                        outputWriter.outputAccountBalanceResponse(abr);
                    }
                    if (newEvent.getType() == 0) {
                        PositionReportEvent pre = (PositionReportEvent) newEvent;
                        // only travel lanes count for accidents
                        if (pre.speed == 0 && pre.lane >= 1 && pre.lane <= 3 ) {
                            cepRT.sendEvent(new StoppedCarEvent(pre));
                        }
                        if (pre.lane == 0) {
                            // entrance lane, we need to calculate the toll
                            // no need to assess toll, because there was no previous segment
                            ChangedSegmentEvent cse = new ChangedSegmentEvent(pre);
                            log.debug("Sending changed segment " + cse);
                            cepRT.sendEvent(cse);
                        }
                        // send the PositionReport for the statistics and further ChangedSegmentEvent detection
                        cepRT.sendEvent(pre);
                    }
//                    cepRT.sendEvent(newEvent);
                }
            }
            Thread.sleep(10); // keep it low so we don't waste i.e. 90ms just because of checking 10ms before next second
        }
        System.out.println("sum = " + sum);
        Thread.sleep(30000);
        outputWriter.close();
        dailyExpenditureProcessor.close();
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
        cepRT.sendEvent(new PositionReportEvent((short) 0, 108, (byte)67));
        cepRT.sendEvent(new CurrentTimeEvent(30000));
        cepRT.sendEvent(new PositionReportEvent((short) 0, 108, (byte)67));
        cepRT.sendEvent(new CurrentTimeEvent(60000));
        cepRT.sendEvent(new PositionReportEvent((short) 0, 108, (byte)68));
        Thread.sleep(3000);
        System.exit(0);


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
        // short time, int vid, byte xway, byte lane, byte direction, byte segment, int position
        cepRT.sendEvent(new TrashedCarEvent((short)1, 101, (byte)0, (byte)0, (byte)0, (byte)4, 2333));
        cepRT.sendEvent(new CurrentTimeEvent(1000));
        cepRT.sendEvent(new TrashedCarEvent((short)1, 102, (byte)0, (byte)0, (byte)0, (byte)4, 2333));
        cepRT.sendEvent(new CurrentTimeEvent(3000));
        cepRT.sendEvent(new TrashedCarEvent((short)1, 103, (byte)0, (byte)0, (byte)0, (byte)4, 2333));
        cepRT.sendEvent(new CurrentTimeEvent(40000));
        cepRT.sendEvent(new TrashedCarEvent((short)1, 104, (byte)0, (byte)0, (byte)0, (byte)2, 2333));
        cepRT.sendEvent(new CurrentTimeEvent(61000));
        cepRT.sendEvent(new TrashedCarEvent((short)61, 105, (byte)0, (byte)0, (byte)0, (byte)2, 2333));
//        cepRT.sendEvent(new CurrentTimeEvent(4000));
//        cepRT.sendEvent(new TrashedCarEvent((short)1, 104, (byte)0, (byte)0, (byte)0, (byte)0, 2334));
//        cepRT.sendEvent(new TrashedCarEvent((short)1, 105, (byte)0, (byte)0, (byte)0, (byte)3, 2334));
//        cepRT.sendEvent(new TrashedCarEvent((short)1, 111, (byte)0, (byte)0, (byte)0, (byte)5, 883));
//        cepRT.sendEvent(new TrashedCarEvent((short)1, 112, (byte)0, (byte)0, (byte)0, (byte)5, 883));
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
