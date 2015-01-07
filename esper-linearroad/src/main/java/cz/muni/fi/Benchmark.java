package cz.muni.fi;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Properties;

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

/**
 * Main class, reads benchmark properties, instantiates Esper, defines queries, registers listener,
 * and runs the event loop.
 *
 * Expects a single argument: -props=<path-to-benchmark-properties>
 * with these properties (values are just examples):
 *
 * BENCH_INPUTFILE=/tmp/cardatapoints.out             #input file
 * BENCH_XWAYS=15                                     #number of expressways in this simulation
 * BENCH_OUTPUTDIRECTORY=/tmp/esper-linearroad/esper/ #output files directory
 * BENCH_DBURL=jdbc:postgresql://localhost/lrb        #database url
 * BENCH_DBUSERNAME=lrb
 * BENCH_DBPASSWORD=lrb
 *
 * For detailed logging, edit the log4j.xml file, logger for cz.muni.fi.
 */
public class Benchmark {

    private static org.apache.log4j.Logger log = Logger.getLogger(Benchmark.class);

    DataDriver datadriver;
    public static int NUM_XWAYS = 0;

    public static void main(String[] args) throws InterruptedException, IOException {
        Benchmark b = new Benchmark();
        b.run(args);
    }

    public void run(String[] args) throws InterruptedException, IOException {

        Properties properties = new Properties();
        for (String arg : args) {
            if (arg.startsWith("-props=")) {
                String filename = arg.split("=")[1];
                FileReader fileReader = new FileReader(filename);
                properties.load(fileReader);
            }
        }
        System.out.println("p = " + properties);
        if (properties.size() == 0) {
            throw new RuntimeException("Provide the benchmark properties with -props=/path/to/file");
        }

        NUM_XWAYS = Integer.parseInt(properties.getProperty("BENCH_XWAYS"));
        assert NUM_XWAYS > 0;

        Configuration cepConfig = new Configuration();

        cepConfig.addEventType("PositionReport", PositionReportEvent.class.getName());
        cepConfig.addEventType("ChangedSegment", ChangedSegmentEvent.class.getName());

        cepConfig.addEventType("InitialSpeed", InitialAverageSpeedEvent.class.getName());
        cepConfig.addEventType("Speed", AverageSpeedEvent.class.getName());
        cepConfig.addEventType("CountStats", CountEvent.class.getName());
        cepConfig.addEventType("Toll", TollEvent.class.getName());

        cepConfig.addEventType("StoppedCar", StoppedCarEvent.class.getName());
        cepConfig.addEventType("TrashedCar", TrashedCarEvent.class.getName());
        cepConfig.addEventType("Accident", AccidentEvent.class.getName());

        EPServiceProvider cep = EPServiceProviderManager.getProvider(null, cepConfig);
        EPAdministrator cepAdm = cep.getEPAdministrator();

        datadriver = new DataDriver(properties.getProperty("BENCH_INPUTFILE"));

        final EPRuntime cepRT =  cep.getEPRuntime();
        final OutputWriter outputWriter = new OutputWriterImpl(datadriver, properties.getProperty("BENCH_OUTPUTDIRECTORY"));
        final AssessmentProcessor assessmentProcessor = new AssessmentProcessor();
        String dbUrl = properties.getProperty("BENCH_DBURL");
        String username = properties.getProperty("BENCH_DBUSERNAME");
        String pass = properties.getProperty("BENCH_DBPASSWORD");
        final DailyExpenditureProcessor dailyExpenditureProcessor = new DailyExpenditureProcessor(null, outputWriter, dbUrl, username, pass);

        //////////////////////////////////////////////////////////////////////////////////
        //////////////////////////////////// QUERIES /////////////////////////////////////
        //////////////////////////////////////////////////////////////////////////////////

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

        // even if a car sends 4 reports with speed 0, it's position may change => not a trashed car
        EPStatement trashedCars = cepAdm.createPattern(
                  " (every pr0=StoppedCar) " +
                          "-> pr1=StoppedCar(vid=pr0.vid and position=pr0.position) where timer:within(35 sec) " +
                          "-> pr2=StoppedCar(vid=pr0.vid and position=pr0.position) where timer:within(35 sec) " +
                          "-> pr3=StoppedCar(vid=pr0.vid and position=pr0.position) where timer:within(35 sec)");
        trashedCars.addListener(new TrashedCarListener(cepRT));

        // distinct filters multiple-car accidents, because a new car matches with all cars in the accident
        EPStatement accidents = cepAdm.createEPL(
                "select distinct Math.floor(t2.time/60)+1 as min, t1.xway as xway, t1.segment as segment, t1.direction as direction, t1.position as position " +
                "from TrashedCar.win:time(30 sec) as t1 " +
                "inner join TrashedCar.win:length(1) as t2 on t1.xway=t2.xway and t1.segment=t2.segment and t1.direction=t2.direction and t1.position=t2.position " +
                "where t1.vid != t2.vid "
        );
        accidents.addListener(new AccidentListener(cepRT));

        // stream of 'new' cars - position reports which changed the segment of a car
        // this works like this: the 35s window has 2 consecutive reports for each car, the current one (=latest one) and the previous
        // the 2nd window keeps just the latest one and compares it only with the previous (the where condition fails when compared with itself)
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
                sum += newEvents.size();
                System.out.println("sum = " + sum + ", sec: " + newEvents.getFirst().getTime());
                for (Event newEvent : newEvents) {
                    if (newEvent.getType() == 3) {
                        // send DailyExpenditureQueries directly to the DEProcessor
                        dailyExpenditureProcessor.handleQuery((DailyExpenditureQuery) newEvent);
                    }
                    if (newEvent.getType() == 2) {
                        // handle AccoutBalanceQueries directly here
                        AccountBalanceQuery abq = (AccountBalanceQuery) newEvent;
                        AssessmentProcessor.Balance balance = assessmentProcessor.getBalance(abq.getVid());
                        AccountBalanceResponse abr = new AccountBalanceResponse(abq.getTime(), abq.getQid(), balance.balance, balance.lastUpdated);
                        outputWriter.outputAccountBalanceResponse(abr);
                    }
                    if (newEvent.getType() == 0) {
                        PositionReportEvent pre = (PositionReportEvent) newEvent;
                        // only travel lanes count for accidents
                        if (pre.speed == 0 && pre.lane >= 1 && pre.lane <= 3 ) {
                            // we do the filtering of StoppedCars here, doing it in Esper would be pointless, since
                            // it is just a simple if
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
                }
            }
            Thread.sleep(10); // keep it low so we don't waste i.e. 90ms just because of checking 10ms before next second
        }
        System.out.println("sum = " + sum);
        Thread.sleep(30000);
        outputWriter.close();
        dailyExpenditureProcessor.close();
    }

}
