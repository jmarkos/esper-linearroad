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
 * This demo scheduler make sure a spout named <code>special-spout</code> in topology <code>special-topology</code> runs
 * on a supervisor named <code>special-supervisor</code>. supervisor does not have name? You can configure it through
 * the config: <code>supervisor.scheduler.meta</code> -- actually you can put any config you like in this config item.
 *
 * In our example, we need to put the following config in supervisor's <code>storm.yaml</code>:
 * <pre>
 *     # give our supervisor a name: "special-supervisor"
 *     supervisor.scheduler.meta:
 *       name: "special-supervisor"
 * </pre>
 *
 * Put the following config in <code>nimbus</code>'s <code>storm.yaml</code>:
 * <pre>
 *     # tell nimbus to use this custom scheduler
 *     storm.scheduler: "storm.DemoScheduler"
 * </pre>
 * @author xumingmingv May 19, 2012 11:10:43 AM
 */
public class CustomScheduler implements IScheduler {
    public void prepare(Map conf) {}

    public void schedule(Topologies topologies, Cluster cluster) {
//        System.out.println("DemoScheduler: begin scheduling");
        // Gets the topology which we want to schedule
        TopologyDetails topology = topologies.getByName("linearroad-topology");

        // make sure the special topology is submitted,
        if (topology != null) {
            boolean needsScheduling = cluster.needsScheduling(topology);

            if (!needsScheduling) {
//                System.out.println("Our special topology DOES NOT NEED scheduling.");
            } else {
                System.out.println("Our special topology needs scheduling.");
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
                    System.out.println("Our dataSpout DOES NOT NEED scheduling.");
                } else {
                    if (!componentToExecutors.containsKey("dailyExpenditureBolt")) {
                        System.out.println("Our dailyExpenditureBolt DOES NOT NEED scheduling.");
                    } else {
                        System.out.println("Our dailyExpenditureBolt needs scheduling.");
                    }
                    System.out.println("Our dataSpout needs scheduling.");
                    List<ExecutorDetails> executors = componentToExecutors.get("dataSpout");
                    List<ExecutorDetails> deExecutors = componentToExecutors.get("dailyExpenditureBolt");
                    executors.addAll(deExecutors);
                    // find out the our "nimbus-supervisor" from the supervisor metadata
                    Collection<SupervisorDetails> supervisors = cluster.getSupervisors().values();
                    SupervisorDetails nimbusSupervisor = null;
                    for (SupervisorDetails supervisor : supervisors) {
                        Map meta = (Map) supervisor.getSchedulerMeta();

                        if (meta.get("name").equals("nimbus-supervisor")) {
                            nimbusSupervisor = supervisor;
                            break;
                        }
                    }

                    // found the nimbus supervisor
                    if (nimbusSupervisor != null) {
                        System.out.println("Found the nimbus-supervisor");
                        List<WorkerSlot> availableSlots = cluster.getAvailableSlots(nimbusSupervisor);

                        // if there is no available slots on this supervisor, free some.
                        // TODO for simplicity, we free all the used slots on the supervisor.
                        if (availableSlots.isEmpty() && !executors.isEmpty()) {
                            System.out.println("No slots available???");
//                            for (Integer port : cluster.getUsedPorts(nimbusSupervisor)) {
//                                cluster.freeSlot(new WorkerSlot(nimbusSupervisor.getId(), port));
//                            }
                        }

                        // re-get the availableSlots
                        availableSlots = cluster.getAvailableSlots(nimbusSupervisor);
                        // since it is just a demo, to keep things simple, we assign all the
                        // executors into one slot.
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