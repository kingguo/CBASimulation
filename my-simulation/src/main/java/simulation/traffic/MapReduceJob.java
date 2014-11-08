package simulation.traffic;


import edu.princeton.cs.introcs.StdRandom;
import simulation.Simulator;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by zguo on 11/4/14.
 */
public class MapReduceJob {
    private static long mapReduceID = 0;
    private long ID;
    private Set<Flow> flows;
    private long arrivalTime;
    private static int[] servers;
    private static TrafficStatus trafficStatusMonitor;

    static {
        servers = new int[Simulator.NETWORK_SIZE];
        for (int i = 0; i < servers.length; i++) {
            servers[i] = i;
        }
        trafficStatusMonitor = Simulator.TRAFFIC_STATUS;
    }

    public MapReduceJob(int numOfTasks, int volumnPerTask, long arrivalTime) {
        ID = mapReduceID++;
        this.arrivalTime = arrivalTime;
        flows = new HashSet<Flow>(numOfTasks * numOfTasks);
        StdRandom.shuffle(servers);
        for (int i = 0; i < numOfTasks; i++) {
            for (int j = 0; j < numOfTasks; j++) {
                if (j != i) {
                    Flow subFlow = new Flow(servers[i], servers[j], volumnPerTask, arrivalTime);
                    subFlow.registerJob(this);
                    flows.add(subFlow);
                }
            }
        }
        trafficStatusMonitor.addJob(this);
    }

    public Set<Flow> getFlows() {
        return flows;
    }

    public long getArrivalTime() {
        return arrivalTime;
    }

    public void removeFlow(Flow flow) {
        boolean sucessRemoval = flows.remove(flow);
        if (!sucessRemoval) {
            throw new IllegalArgumentException("flow not found in mapreduce job!");
        }
        if (jobFinished()) {
            trafficStatusMonitor.removeJob(this);
        }
    }

    public boolean jobFinished() {
        return flows.isEmpty();
    }

    public long getID() {
        return ID;
    }
}
