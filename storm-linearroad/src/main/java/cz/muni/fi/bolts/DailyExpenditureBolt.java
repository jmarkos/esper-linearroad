package cz.muni.fi.bolts;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import cz.muni.fi.DailyExpenditureProcessor;
import cz.muni.fi.OutputWriter;
import cz.muni.fi.TimeService;
import cz.muni.fi.eventtypes.AccidentNotificationEvent;
import cz.muni.fi.eventtypes.AccountBalanceResponse;
import cz.muni.fi.eventtypes.DailyExpenditureQuery;
import cz.muni.fi.eventtypes.DailyExpenditureResponse;
import cz.muni.fi.eventtypes.TollNotificationEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DailyExpenditureBolt extends BaseRichBolt {
    OutputCollector _collector;
    DailyExpenditureProcessor dailyExpenditureProcessor;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
        String outputDirectory = (String) conf.get("BENCH_OUTPUTDIRECTORY");
        String dbUrl = (String) conf.get("BENCH_DBURL");
        String user = (String) conf.get("BENCH_DBUSERNAME");
        String password = (String) conf.get("BENCH_DBPASSWORD");
        int taskId = context.getThisTaskId();
        TimeService timeService = new TimeService((Long) conf.get("BENCH_STARTTIME"));
        dailyExpenditureProcessor = new DailyExpenditureProcessor(null, new DailyExpenditureOutputWriter(outputDirectory, taskId, timeService), dbUrl, user, password);
    }

    @Override
    public void execute(Tuple tuple) {
        // DailyExpenditureQuery = byte type, short time, int vid, byte xway, int qid, byte day
        DailyExpenditureQuery deq = new DailyExpenditureQuery();
        deq.type = tuple.getByte(0);
        deq.time = tuple.getShort(1);
        deq.vid = tuple.getInteger(2);
        deq.xway = tuple.getByte(3);
        deq.qid = tuple.getInteger(4);
        deq.dayy = tuple.getByte(5);
        dailyExpenditureProcessor.handleQuery(deq);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // no output streams
    }

    class DailyExpenditureOutputWriter implements OutputWriter {

        public Logger log = LoggerFactory.getLogger(DailyExpenditureOutputWriter.class);

        public String outputDirectory;
        public String dailyExpenditureResponsesFileName;
        PrintWriter dailyExpendituresWriter;
        TimeService timeService;

        public DailyExpenditureOutputWriter(String _outputDirectory, int taskId, TimeService timeService) {
            this.outputDirectory = _outputDirectory;
            this.timeService = timeService;
            this.dailyExpenditureResponsesFileName = "dailyexpenditures" + taskId + ".txt";
            try {
                dailyExpendituresWriter = new PrintWriter(new FileWriter(outputDirectory + dailyExpenditureResponsesFileName), true);
            } catch (IOException e) {
                throw new RuntimeException("Creating file for output DE events failed. ", e);
            }
        }

        @Override
        public void outputAccidentNotification(AccidentNotificationEvent ane) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void outputTollNotification(TollNotificationEvent tne) {
            throw new UnsupportedOperationException();
        }

        @Override
        public synchronized void outputDailyExpenditureResponse(DailyExpenditureResponse der) {
            der.setOutputTime((short) timeService.getTime());
            log.debug("Writing daily expenditure response {}", der);
            dailyExpendituresWriter.println(der.toFileString());
        }

        @Override
        public void outputAccountBalanceResponse(AccountBalanceResponse abr) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
            dailyExpendituresWriter.close();
        }
    }

}
