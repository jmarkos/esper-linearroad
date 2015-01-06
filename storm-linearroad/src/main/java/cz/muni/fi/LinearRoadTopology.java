package cz.muni.fi;

import java.io.FileReader;
import java.util.Map;
import java.util.Properties;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;
import cz.muni.fi.bolts.AccidentDetectionBolt;
import cz.muni.fi.bolts.AccountsBolt;
import cz.muni.fi.bolts.ChangedSegmentBolt;
import cz.muni.fi.bolts.DailyExpenditureBolt;
import cz.muni.fi.bolts.NotificationsBolt;
import cz.muni.fi.bolts.TollsBolt;
import cz.muni.fi.spouts.DataSpout;


/**
 * Benchmark properties:
 *
 * BENCH_INPUTFILE       - file with the input data
 * BENCH_HISTFILE        - contains the
 * BENCH_XWAYS           - number of expressways used in the input file
 * BENCH_OUTPUTDIRECTORY - directory where the output files will be saved
 * BENCH_DBURL           - database jdbc url with the historical data
 * BENCH_DBUSERNAME      - database username
 * BENCH_DBPASSWORD      - database password
 *
 */
public class LinearRoadTopology {

    public static class CountPRBolt extends BaseRichBolt {
        OutputCollector _collector;

        int taskId;

        @Override
        public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
            _collector = collector;
            this.taskId = context.getThisTaskId();
        }

        int count = 0;
        short currentTime = 0;

        @Override
        public void execute(Tuple tuple) {
            count++;
            short receivedTime = tuple.getShort(1);
            if (receivedTime > currentTime)
                currentTime = receivedTime;
            if (receivedTime < currentTime)
                System.out.println("Ordering broken!");
            if (count % 100000 == 0) {
                System.out.println("CountPRBolt" + taskId + " count: " + count);
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
//            declarer.declare(new Fields("word"));
        }

    }

    public static void main(String[] args) throws Exception {

        boolean localmode = false;

        Properties properties = new Properties();
        for (String arg : args) {
            if (arg.startsWith("-props=")) {
                String filename = arg.split("=")[1];
                FileReader fileReader = new FileReader(filename);
                properties.load(fileReader);
            }
            if (arg.equals("-localmode")) {
                localmode = true;
            }
        }

        System.out.println("properties = " + properties);
        int num_xways = Integer.parseInt(properties.getProperty("BENCH_XWAYS"));


        Config conf = new Config();
//        conf.setDebug(true);
        conf.setNumAckers(0);

        // pass the configuration to all spouts/bolts
        for (Object o : properties.keySet()) {
            conf.put((String)o, properties.get(o));
        }

        if (localmode) {
            TopologyBuilder builder = new TopologyBuilder();

            builder.setSpout("dataSpout", new DataSpout(), 1);
//            builder.setBolt("print", new CountPRBolt(), 1).fieldsGrouping("dataSpout", "positionReport", new Fields("xway"));
//        builder.setBolt("exclaim2", new ExclamationBolt(), 2).shuffleGrouping("exclaim1");
            builder.setBolt("dailyExpenditureBolt", new DailyExpenditureBolt(), 1)
                    .allGrouping("dataSpout", "dailyExpenditureQuery");
            builder.setBolt("accidentDetectionBolt", new AccidentDetectionBolt(), 1)
                    .fieldsGrouping("dataSpout", "stoppedCar", new Fields("xway"));
            // TODO maybe just 2 for both tolls/chs
            builder.setBolt("tollsBolt", new TollsBolt(), 1)
                    .fieldsGrouping("dataSpout", "positionReport", new Fields("xway"))
                    .fieldsGrouping("accidentDetectionBolt", "accident", new Fields("xway"));
            // TODO could be possibly grouped by vid? as well as accidentbolt, but since cars dont change xways, xway gives us a better distribution
            builder.setBolt("changedSegmentBolt", new ChangedSegmentBolt(), 1)
                    .fieldsGrouping("dataSpout", "positionReport", new Fields("xway"));
            builder.setBolt("notificationsBolt", new NotificationsBolt(), 1)
                    .fieldsGrouping("tollsBolt", "toll", new Fields("xway"))
                    .fieldsGrouping("changedSegmentBolt", "changedSegment", new Fields("xway"));
            builder.setBolt("accountsBolt", new AccountsBolt(), 1)
                    .fieldsGrouping("dataSpout", "accountBalanceQuery", new Fields("vid"))
                    .fieldsGrouping("notificationsBolt", new Fields("vid"));

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("test", conf, builder.createTopology());
            Utils.sleep(2000000); // more than 30 min
            cluster.killTopology("test");
            cluster.shutdown();
        }
        else {
            conf.setNumWorkers(3);
            TopologyBuilder builder = new TopologyBuilder();

            builder.setSpout("dataSpout", new DataSpout(), 1);
//            builder.setBolt("print", new CountPRBolt(), 1).fieldsGrouping("dataSpout", "positionReport", new Fields("xway"));
//        builder.setBolt("exclaim2", new ExclamationBolt(), 2).shuffleGrouping("exclaim1");
            builder.setBolt("dailyExpenditureBolt", new DailyExpenditureBolt(), 1)
                    .allGrouping("dataSpout", "dailyExpenditureQuery");
            builder.setBolt("accidentDetectionBolt", new AccidentDetectionBolt(), 2)
                    .fieldsGrouping("dataSpout", "stoppedCar", new Fields("xway"));
            // TODO maybe just 2 for both tolls/chs
            builder.setBolt("tollsBolt", new TollsBolt(), 4)
                    .fieldsGrouping("dataSpout", "positionReport", new Fields("xway"))
                    .fieldsGrouping("accidentDetectionBolt", "accident", new Fields("xway"));
            // TODO could be possibly grouped by vid? as well as accidentbolt, but since cars dont change xways, xway gives us a better distribution
            builder.setBolt("changedSegmentBolt", new ChangedSegmentBolt(), 2)
                    .fieldsGrouping("dataSpout", "positionReport", new Fields("xway"));
            builder.setBolt("notificationsBolt", new NotificationsBolt(), 2)
                    .fieldsGrouping("tollsBolt", "toll", new Fields("xway"))
                    .fieldsGrouping("changedSegmentBolt", "changedSegment", new Fields("xway"));
            builder.setBolt("accountsBolt", new AccountsBolt(), 2)
                    .fieldsGrouping("dataSpout", "accountBalanceQuery", new Fields("vid"))
                    .fieldsGrouping("notificationsBolt", new Fields("vid"));

            long time = System.currentTimeMillis() / 1000;
            long startTime = ((time + 20) / 10) * 10;
            conf.put("BENCH_STARTTIME", startTime);
            System.out.println("current time: " + time);
            System.out.println("starttime: " + startTime);
            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        }
    }
}
