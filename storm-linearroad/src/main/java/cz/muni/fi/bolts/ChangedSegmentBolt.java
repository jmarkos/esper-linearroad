package cz.muni.fi.bolts;

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
import cz.muni.fi.eventtypes.ChangedSegmentEvent;
import cz.muni.fi.eventtypes.PositionReportEvent;
import cz.muni.fi.eventtypes.StoppedCarEvent;
import cz.muni.fi.eventtypes.TrashedCarEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChangedSegmentBolt  extends BaseRichBolt {
    public static Logger log = LoggerFactory.getLogger(ChangedSegmentBolt.class);
    OutputCollector _collector;

    EPAdministrator cepAdm;
    EPRuntime cepRT;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
        int taskId = context.getThisTaskId();
        String componentId = context.getThisComponentId();

        Configuration cepConfig = new Configuration();
        cepConfig.addEventType("PositionReport", PositionReportEvent.class.getName());
        EPServiceProvider cep = EPServiceProviderManager.getProvider(componentId + taskId, cepConfig);
        cepAdm = cep.getEPAdministrator();
        cepRT =  cep.getEPRuntime();

        EPStatement changedSegment = cepAdm.createEPL(
                "select pr2.time as time, pr2.vid as vid, pr1.segment as oldSegment, pr2.segment as newSegment, pr2.xway as xway, pr2.direction as direction, pr2.lane as lane " +
                        "from PositionReport.win:time(35 sec) as pr1 " +
                        "inner join PositionReport.win:length(1) as pr2 on pr1.vid=pr2.vid and pr1.xway=pr2.xway " +
                        "where pr2.segment!=pr1.segment ");
        changedSegment.addListener(new UpdateListener() {
            @Override
            public void update(EventBean[] newEvents, EventBean[] oldEvents) {
                if (newEvents.length > 1) {
                    log.warn("Changed segment listener received more than 1 new events.");
                }
                for (EventBean newEvent : newEvents) {
                    ChangedSegmentEvent cse = new ChangedSegmentEvent(newEvent);
                    log.debug("Sending changed segment {}", cse);
//                    System.out.println("Sending changed segment " + cse);
                    // ChangedSegmentEvent = short time, int min, int vid, byte xway, byte lane, byte direction, byte oldSegment, byte newSegment
                    _collector.emit("changedSegment", new Values((short) cse.time, cse.min, cse.vid, (byte) cse.xway, (byte) cse.lane, (byte) cse.direction, (byte) cse.oldSegment, (byte) cse.newSegment));
                }
            }
        });
    }

    @Override
    public void execute(Tuple tuple) {
        // this bolt is getting PositionReportEvents
        // PositionReportEvent = byte type, short time, int vid, byte speed, byte xway, byte lane, byte direction, byte segment, int position
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
        cepRT.sendEvent(pre);
        if (pre.lane == 0) {
            ChangedSegmentEvent cse = new ChangedSegmentEvent(pre);
            _collector.emit("changedSegment", new Values((short) cse.time, cse.min, cse.vid, (byte) cse.xway, (byte) cse.lane, (byte) cse.direction, (byte) cse.oldSegment, (byte) cse.newSegment));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // ChangedSegmentEvent = short time, int min, int vid, byte xway, byte lane, byte direction, byte oldSegment, byte newSegment
        declarer.declareStream("changedSegment", new Fields("time", "min", "vid", "xway", "lane", "direction", "oldSegment", "newSegment"));
    }

}
