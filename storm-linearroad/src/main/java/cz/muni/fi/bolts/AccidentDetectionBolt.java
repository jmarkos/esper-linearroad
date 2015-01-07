package cz.muni.fi.bolts;

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
import cz.muni.fi.eventtypes.LRBEvent;
import cz.muni.fi.eventtypes.StoppedCarEvent;
import cz.muni.fi.eventtypes.TrashedCarEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AccidentDetectionBolt extends BaseRichBolt {
    public static Logger log = LoggerFactory.getLogger(AccidentDetectionBolt.class);

    OutputCollector _collector;

    EPAdministrator cepAdm;
    EPRuntime cepRT;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
        int taskId = context.getThisTaskId();
        String componentId = context.getThisComponentId();
        Configuration cepConfig = new Configuration();
        cepConfig.addEventType("StoppedCar", StoppedCarEvent.class.getName());
        cepConfig.addEventType("TrashedCar", TrashedCarEvent.class.getName());
        EPServiceProvider cep = EPServiceProviderManager.getProvider(componentId + taskId, cepConfig);
        cepAdm = cep.getEPAdministrator();
        cepRT =  cep.getEPRuntime();

        EPStatement trashedCars = cepAdm.createPattern(
                " (every pr0=StoppedCar) " +
                        "-> pr1=StoppedCar(vid=pr0.vid and position=pr0.position) where timer:within(35 sec) " +
                        "-> pr2=StoppedCar(vid=pr0.vid and position=pr0.position) where timer:within(35 sec) " +
                        "-> pr3=StoppedCar(vid=pr0.vid and position=pr0.position) where timer:within(35 sec)");
        trashedCars.addListener(new UpdateListener() {
            @Override
            public void update(EventBean[] newEvents, EventBean[] oldEvents) {
                TrashedCarEvent tce = new TrashedCarEvent();
                StoppedCarEvent sce = (StoppedCarEvent) newEvents[0].get("pr3");
                tce.time = sce.time;
                tce.vid = sce.vid;
                tce.xway = sce.xway;
                tce.segment = sce.segment;
                tce.direction = sce.direction;
                tce.position = sce.position;
                log.debug("Sending trashed car {}", tce);
                cepRT.sendEvent(tce);
            }
        });

        EPStatement accidents = cepAdm.createEPL(
                "select distinct Math.floor(t2.time/60)+1 as min, t1.xway as xway, t1.segment as segment, t1.direction as direction, t1.position as position " +
                        "from TrashedCar.win:time(30 sec) as t1 " +
                        "inner join TrashedCar.win:length(1) as t2 on t1.xway=t2.xway and t1.segment=t2.segment and t1.direction=t2.direction and t1.position=t2.position " +
                        "where t1.vid != t2.vid "
        );
        accidents.addListener(new UpdateListener() {
            @Override
            public void update(EventBean[] newEvents, EventBean[] oldEvents) {
                if (newEvents == null) {
                    return;
                }
                for (EventBean accident : newEvents) {
                    int min = ((Double)newEvents[0].get("min")).intValue();
                    byte xway = (Byte)accident.get("xway");
                    byte direction = (Byte)accident.get("direction");
                    byte segment = (Byte)accident.get("segment");
                    int position = (Integer)accident.get("position");
                    // EAST, moving to the right, so we need to notify the upstream segments on the left (lower segment)
                    if (direction == 0) {
                        for (int i = 0; i < 5; i++) {
                            if (segment - i < 0) {
                                break;
                            }
                            log.debug("Sending accident: {}, {}, {}, {}, {}, {}", min, xway, direction, segment - i, segment, position);
                            // AccidentEvent = int min, byte xway, byte direction, byte segment, byte originalSegment, int position
                            _collector.emit("accident", new Values(min, xway, direction, (byte) (segment - i), (byte) segment, position));
                        }
                    }

                    // WEST, moving to the left, notifying upstream segments on the right (higher segment)
                    if (direction == 1) {
                        for (int i = 0; i < 5; i++) {
                            if (segment + i > 99) {
                                break;
                            }
                            log.debug("Sending accident: {}, {}, {}, {}, {}, {}", min, xway, direction, segment + i, segment, position);
                            // AccidentEvent = int min, byte xway, byte direction, byte segment, byte originalSegment, int position
                            _collector.emit("accident", new Values(min, xway, direction, (byte) (segment + i), (byte) segment, position));
                        }
                    }
                }
            }
        });
    }

    @Override
    public void execute(Tuple tuple) {
        // StoppedCarEvent = short time, int vid, byte xway, byte lane, byte direction, byte segment, int position
        StoppedCarEvent sce = new StoppedCarEvent();
        sce.time = tuple.getShort(0);
        sce.vid = tuple.getInteger(1);
        sce.xway = tuple.getByte(2);
        sce.lane = tuple.getByte(3);
        sce.direction = tuple.getByte(4);
        sce.segment = tuple.getByte(5);
        sce.position = tuple.getInteger(6);
        cepRT.sendEvent(sce);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // AccidentEvent = int min, byte xway, byte direction, byte segment, byte originalSegment, int position
        declarer.declareStream("accident", new Fields("min", "xway", "direction", "segment", "originalSegment", "position"));
    }

}
