package cz.muni.fi.spouts;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import cz.muni.fi.DataDriver;
import cz.muni.fi.eventtypes.AccountBalanceQuery;
import cz.muni.fi.eventtypes.DailyExpenditureQuery;
import cz.muni.fi.eventtypes.Event;
import cz.muni.fi.eventtypes.PositionReportEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataSpout extends BaseRichSpout {
    public static Logger log = LoggerFactory.getLogger(DataSpout.class);
    SpoutOutputCollector _collector;

    long startTime;
    boolean started = false;

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
        String inputFile = (String) conf.get("BENCH_INPUTFILE");
        datadriver = new DataDriver(inputFile);
//        datadriver.setSpeedup(10);
        startTime = (Long) conf.get("BENCH_STARTTIME");
    }

    int num_of_xways = 15;
    int countOfSentPR = 0;

    DataDriver datadriver;


    public void nextTuple() {
        if (!started) {
            long currentTime = System.currentTimeMillis() / 1000;
            if (currentTime >= startTime) {
                started = true;
                datadriver.start();
            } else {
                return;
            }
        }

        if (!datadriver.simulationEnded) {
            ArrayDeque<Event> newEvents = null;
            try {
                newEvents = datadriver.getNewEvents();
            } catch (IOException e) {
//                log.error("Error when reading from DataDriver: ", e);
            }
            if (newEvents != null) {
                for (Event newEvent : newEvents) {
                    if (newEvent.getType() == 3) {
                        DailyExpenditureQuery deq = (DailyExpenditureQuery) newEvent;
                        // DailyExpenditureQuery = byte type, short time, int vid, byte xway, int qid, byte day
                        _collector.emit("dailyExpenditureQuery", new Values((byte) deq.type, (short) deq.time, deq.vid, (byte) deq.xway, deq.qid, (byte) deq.dayy));
                    }
                    if (newEvent.getType() == 2) {
                        AccountBalanceQuery abq = (AccountBalanceQuery) newEvent;
                        // AccountBalanceQuery =  byte type, short time, int vid, int qid
                        _collector.emit("accountBalanceQuery", new Values((byte) abq.type, (short) abq.time, (int) abq.vid, (int) abq.qid));
                    }
                    if (newEvent.getType() == 0) {
                        PositionReportEvent pre = (PositionReportEvent) newEvent;
                        // only travel lanes count for accidents
                        if (pre.speed == 0 && pre.lane >= 1 && pre.lane <= 3 ) {
                            // StoppedCarEvent = short time, int vid, byte xway, byte lane, byte direction, byte segment, int position
                            _collector.emit("stoppedCar", new Values((short) pre.time, pre.vid, (byte) pre.xway, (byte) pre.lane, (byte) pre.direction, (byte) pre.segment, pre.position));
                        }
                        // send the PositionReport for the statistics and further ChangedSegmentEvent detection
                        // PositionReportEvent = byte type, short time, int vid, byte speed, byte xway, byte lane, byte direction, byte segment, int position
                        countOfSentPR++;
                        _collector.emit("positionReport", new Values(pre.type, pre.time, pre.vid, pre.speed, pre.xway, pre.lane, pre.direction, pre.segment, pre.position));
                    }
                }

                log.info("DataSpout sent " + countOfSentPR + " PRs" + " at time " + datadriver.lastSecond);
            }
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // TODO maybe add a toFields method to events
        declarer.declare(new Fields("xway", "segment"));
        // StoppedCarEvent = short time, int vid, byte xway, byte lane, byte direction, byte segment, int position
        declarer.declareStream("stoppedCar", new Fields("time", "vid", "xway", "lane", "direction", "segment", "position"));
        // PositionReportEvent = byte type, short time, int vid, byte speed, byte xway, byte lane, byte direction, byte segment, int position
        declarer.declareStream("positionReport", new Fields("type", "time", "vid", "speed", "xway", "lane", "direction", "segment", "position"));
        // DailyExpenditureQuery = byte type, short time, int vid, byte xway, int qid, byte day
        declarer.declareStream("dailyExpenditureQuery", new Fields("type", "time", "vid", "xway", "qid", "day"));
        // AccountBalanceQuery = byte type, short time, int vid, int qid
        declarer.declareStream("accountBalanceQuery", new Fields("type", "time", "vid", "qid"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> ret = new HashMap<String, Object>();
        ret.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, 1);
        return ret;
    }
}