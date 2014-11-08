package simulation.traffic;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import simulation.Simulator;
import simulation.network.NetworkStatusReporter;
import simulation.network.Route;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import static simulation.Simulator.log;
/**
 * Created by zguo on 11/2/14.
 */
public final class TrafficStatus{

    private long currentTime;
    private Table<Integer, Integer, Set<Flow>> virtualOutputQueues = HashBasedTable.create();
    private Map<Long, MapReduceJob> remainingJobs = new HashMap<Long, MapReduceJob>();
    private Map<Long, Flow> remainingFlows = new HashMap<Long, Flow>();
    private NetworkStatusReporter reporter = new NetworkStatusReporter();
    private TrafficMatrix trafficDemandMatrix = new TrafficMatrix(Simulator.NETWORK_SIZE);

    public void addTraffic(Flow arrival) {
        int src = arrival.getSrc();
        int dst = arrival.getDst();
        if(!virtualOutputQueues.contains(src, dst)){
            virtualOutputQueues.put(src, dst, new HashSet<Flow>());
        }
        virtualOutputQueues.get(src, dst).add(arrival);
        remainingFlows.put(arrival.getID(), arrival);
        assert(numOfFlows() == remainingFlows.size());
    }

    private void addTraffic(MapReduceJob arrival){
        for(Flow flow : arrival.getFlows()){
            addTraffic(flow);
        }
    }
    private double totalDemandInQueue(Set<Flow> flows){
        double volumn = 0;
        for(Flow flow: flows){
            volumn += flow.getRemainingDemand();
        }
        return volumn;
    }

    public void transmit(Route currentSchedule) {
        TrafficMatrix bandwidthMx = currentSchedule.getBandwidthMx(currentTime);
        for(int i = 0; i < bandwidthMx.numRows(); i++){
            for(int j = 0; j < bandwidthMx.numCols(); j++){
                double bandwidth = bandwidthMx.get(i, j);
                for(Flow flows : virtualOutputQueues.get(i, j)){
                    flows.transmit(bandwidth/virtualOutputQueues.get(i, j).size());
                }
        }
    };

    public TrafficMatrix currentTrafficMatrix() {
        trafficDemandMatrix.clear();
        for(Table.Cell<Integer,Integer,Set<Flow>> cell : virtualOutputQueues.cellSet()){
            trafficDemandMatrix.set(cell.getRowKey(), cell.getColumnKey(), totalDemandInQueue(cell.getValue()));
        }
        return trafficDemandMatrix;
    }

    public void addJob(MapReduceJob mapReduceJob) {
        addTraffic(mapReduceJob);
        remainingJobs.put(mapReduceJob.getID(), mapReduceJob);
    }

    public void removeJob(MapReduceJob mapReduceJob) {
        long jobID = mapReduceJob.getID();
        if(!remainingJobs.containsKey(jobID)){
            log("error: job doesn't exist");
            throw new IllegalArgumentException("mapreduce job not in queue");
        }
        if(!mapReduceJob.jobFinished()){
            log("error: job has remaining flows");
            throw new IllegalArgumentException("job has remaming flows");
        }
        long jobLatency = currentTime - mapReduceJob.getArrivalTime();
        reporter.recordJobLatency(mapReduceJob, jobLatency);
        remainingJobs.remove(mapReduceJob.getID());
    }

    public void setTime(long mills){
        currentTime = mills;
    }

    public void removeFlow(Flow flow) {
        if(!virtualOutputQueues.containsValue(flow) || !flow.isEmpty()){
            throw new IllegalArgumentException("flow to remove is not active or flow not finished");
        }
        long latency = currentTime - flow.getArrivalTime();
        reporter.reportFlowLatency(flow, latency);
        remainingFlows.remove(flow.getID());
        virtualOutputQueues.get(flow.getSrc(),flow.getDst()).remove(flow);
        assert(numOfFlows() == remainingFlows.size());
    }

    private long numOfFlows(){
        long size = 0;
        for(Table.Cell<Integer,Integer,Set<Flow>> cell : virtualOutputQueues.cellSet()){
            size += cell.getValue().size();
        }
        return size;
    }
}
