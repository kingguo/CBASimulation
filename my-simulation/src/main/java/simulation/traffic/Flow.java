package simulation.traffic;

import simulation.Simulator;

import static simulation.Simulator.log;
/**
 * Created by zguo on 11/4/14.
 */
public class Flow{
    private static long flowID = 0;
    private long ID;
    private int src;
    private int dst;
    private long arrivalTime;
    private double volumn;
    private MapReduceJob mJob = null;
    private static TrafficStatus trafficStatusMonitor = Simulator.TRAFFIC_STATUS;
    public Flow(int src, int dst, double volumnPerTask, long arrivalTime) {
        ID = flowID;
        flowID++;
        this.src = src;
        this.dst = dst;
        this.volumn = volumnPerTask;
        this.arrivalTime = arrivalTime;
        trafficStatusMonitor.addTraffic(this);
    }

    public double getRemainingDemand() {
        return volumn;
    }

    public long getArrivalTime() {
        return arrivalTime;
    }

    public boolean isEmpty() {
        return volumn == 0;
    }

    public void transmit(double volPerMillSec){
        volumn -= volPerMillSec;
        if(volumn <= 0){
         if(mJob != null){
             mJob.removeFlow(this);
             trafficStatusMonitor.removeFlow(this);
         }
        }
    }

    public void registerJob(MapReduceJob mapReduceJob) {
        if(mJob != null){
            log("flow is already assaigned to some mapreduce job");
            throw new IllegalArgumentException("shit hits the fan");
        }
        mJob = mapReduceJob;
    }

    public int getSrc() {
        return src;
    }

    public int getDst() {
        return dst;
    }

    public long getID() {
        return ID;
    }
}
