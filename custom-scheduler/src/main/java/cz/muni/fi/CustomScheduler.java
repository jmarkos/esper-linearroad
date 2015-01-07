package cz.muni.fi;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.EvenScheduler;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.IScheduler;
import backtype.storm.scheduler.SchedulerAssignment;
import backtype.storm.scheduler.SupervisorDetails;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.WorkerSlot;

/**
 *
 * https://xumingming.sinaapp.com/885/twitter-storm-how-to-develop-a-pluggable-scheduler/
 * and
 * https://github.com/xumingming/storm-lib/blob/master/src/jvm/storm/DemoScheduler.java
 *
 * @author xumingmingv May 19, 2012 11:10:43 AM
 * @author jmarkos Dec, 2014
 */
public class CustomScheduler implements IScheduler {
    public void prepare(Map conf) {}

    public void schedule(Topologies topologies, Cluster cluster) {
        // Gets the topology which we want to schedule
        TopologyDetails topology = topologies.getByName("linearroad-topology");

        // make sure the LRB topology is submitted,
        if (topology != null) {
            boolean needsScheduling = cluster.needsScheduling(topology);

            if (!needsScheduling) {
            } else {
                System.out.println("LRB topology needs scheduling.");
                // find out all the needs-scheduling components of this topology
                Map<String, List<ExecutorDetails>> componentToExecutors = cluster.getNeedsSchedulingComponentToExecutors(topology);

                System.out.println("needs scheduling(component->executor): " + componentToExecutors);
                System.out.println("needs scheduling(executor->compoenents): " + cluster.getNeedsSchedulingExecutorToComponents(topology));
                SchedulerAssignment currentAssignment = cluster.getAssignmentById(topology.getId());
                if (currentAssignment != null) {
                    System.out.println("current assignments: " + currentAssignment.getExecutorToSlot());
                } else {
                    System.out.println("current assignments: {}");
                }

                // dataSpout, dailyExpenditureBolt
                if (!componentToExecutors.containsKey("dataSpout")) {
                    System.out.println("dataSpout DOES NOT NEED scheduling.");
                } else {
                    if (!componentToExecutors.containsKey("dailyExpenditureBolt")) {
                        System.out.println("dailyExpenditureBolt DOES NOT NEED scheduling.");
                    } else {
                        System.out.println("dailyExpenditureBolt needs scheduling.");
                    }
                    List<ExecutorDetails> executors = componentToExecutors.get("dataSpout");
                    List<ExecutorDetails> deExecutors = componentToExecutors.get("dailyExpenditureBolt");
                    executors.addAll(deExecutors);
                    // find out our "nimbus-supervisor" from the supervisor metadata
                    Collection<SupervisorDetails> supervisors = cluster.getSupervisors().values();
                    SupervisorDetails nimbusSupervisor = null;
                    for (SupervisorDetails supervisor : supervisors) {
                        Map meta = (Map) supervisor.getSchedulerMeta();

                        if (meta.get("name").equals("nimbus-supervisor")) {
                            nimbusSupervisor = supervisor;
                            break;
                        }
                    }

                    if (nimbusSupervisor != null) {
                        System.out.println("Found the nimbus-supervisor");
                        List<WorkerSlot> availableSlots = cluster.getAvailableSlots(nimbusSupervisor);

                        if (availableSlots.isEmpty() && !executors.isEmpty()) {
                            System.out.println("No slots available? Make sure there is space in the cluster.");
                        }

                        cluster.assign(availableSlots.get(0), topology.getId(), executors);
                        System.out.println("We assigned executors:" + executors + " to slot: [" + availableSlots.get(0).getNodeId() + ", " + availableSlots.get(0).getPort() + "]");
                    } else {
                        System.out.println("There is no supervisor named nimbus-supervisor!!!");
                    }
                }


            }
        }

        // let system's even scheduler handle the rest scheduling work
        // you can also use your own other scheduler here, this is what
        // makes storm's scheduler composable.
        new EvenScheduler().schedule(topologies, cluster);
    }

}