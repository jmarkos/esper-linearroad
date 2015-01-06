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
import com.espertech.esper.client.EPAdministrator;
import com.espertech.esper.client.EPRuntime;
import com.espertech.esper.client.EPStatement;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;
import cz.muni.fi.AssessmentProcessor;
import cz.muni.fi.TimeService;
import cz.muni.fi.eventtypes.AccidentNotificationEvent;
import cz.muni.fi.eventtypes.AccountBalanceQuery;
import cz.muni.fi.eventtypes.AccountBalanceResponse;
import cz.muni.fi.eventtypes.ChangedSegmentEvent;
import cz.muni.fi.eventtypes.TollEvent;
import cz.muni.fi.eventtypes.TollNotificationEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AccountsBolt extends BaseRichBolt {
    public static Logger log = LoggerFactory.getLogger(AccountsBolt.class);
    OutputCollector _collector;
    AssessmentProcessor assessmentProcessor;
    PrintWriter accountBalanceWriter;
    TimeService timeService;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
        assessmentProcessor = new AssessmentProcessor();
        timeService = new TimeService((Long) conf.get("BENCH_STARTTIME"));
        int taskId = context.getThisTaskId();
        String outputDirectory = (String) conf.get("BENCH_OUTPUTDIRECTORY");
        String accountBalanceResponsesFileName = "accountbalances" + taskId + ".txt";
        try {
            accountBalanceWriter = new PrintWriter(new FileWriter(outputDirectory + accountBalanceResponsesFileName), true);
        } catch (IOException e) {
            throw new RuntimeException("Creating file for output Account Balance events failed. ", e);
        }
    }

    private void handleQuery(AccountBalanceQuery abq) {
        AssessmentProcessor.Balance balance = assessmentProcessor.getBalance(abq.vid);
        AccountBalanceResponse abr = new AccountBalanceResponse(abq.getTime(), abq.getQid(), balance.balance, balance.lastUpdated);
        abr.setOutputTime((short) timeService.getTime());
        log.debug("Writing account balance response {}", abr);
//        System.out.println("Writing account balance response " + abr);
        accountBalanceWriter.println(abr.toFileString());
        if (accountBalanceWriter.checkError()) {
            log.error("Error writing AccountBalanceResponse to a file.");
        }
    }

    @Override
    public void execute(Tuple tuple) {
        // receives AccountBalanceQueries
        // receives ProcessTollEvent
        String sourceComponent = tuple.getSourceComponent();
        if (sourceComponent.equals("dataSpout")) {
            // AccountBalanceQuery =  byte type, short time, int vid, int qid
            AccountBalanceQuery abq = new AccountBalanceQuery();
            abq.time = tuple.getShort(1);
            abq.vid = tuple.getInteger(2);
            abq.qid = tuple.getInteger(3);
            handleQuery(abq);
        } else { // toll
            // ProcessTollEvent = byte oldSegment, int accSegment, int vid, short time, int toll
            byte oldSegment = tuple.getByte(0);
            int accSegment = tuple.getInteger(1);
            int vid = tuple.getInteger(2);
            short time = tuple.getShort(3);
            int toll = tuple.getInteger(4);
            if (oldSegment != -1) {
                assessmentProcessor.assessToll(vid, time);
            }
            if (accSegment == -1) { // no accident
                assessmentProcessor.rememberToll(vid, toll);
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // no output stream
    }

}
