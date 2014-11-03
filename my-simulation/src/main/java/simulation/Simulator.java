package simulation;

import simulation.network.NetworkScheduler;
import simulation.network.NetworkStatusReporter;
import simulation.network.Route;
import simulation.traffic.TrafficGenerator;
import simulation.traffic.TrafficMatrix;
import simulation.traffic.TrafficStatus;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class Simulator{
    public static final long SIMULATION_DURATION = 100000L;
    private TrafficGenerator trafficGenerator;
    private TrafficStatus trafficStatus;
    private NetworkScheduler scheduler;
    private static NetworkStatusReporter reporter = new NetworkStatusReporter();

    public Simulator(TrafficGenerator generator, NetworkScheduler scheduler) {
        this.trafficGenerator = generator;
        this.scheduler = scheduler;
        trafficStatus = new TrafficStatus();
    }


    public static void main( String[] args )
    {   long timeMills = 0;
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        System.out.println("please input ");
        Simulator simulator = new Simulator();
        long schedulePeriod = 1000L;
        long logInterval = 1000L;

        TrafficGenerator generator = simulator.getTrafficGenerator();
        TrafficStatus network = simulator.getTrafficStatus();
        NetworkScheduler scheduler = simulator.getScheduler();

        TrafficMatrix matrix = null;
        Route currentSchedule = null;
        for(timeMills = 0; timeMills < SIMULATION_DURATION; timeMills++) {
            network.addTraffic(generator.arrival(), timeMills);
            network.transmit(currentSchedule, timeMills);
            if (timeMills % schedulePeriod == 0) {
                matrix = network.currentTrafficMatrix();
                currentSchedule = scheduler.schedule(matrix, timeMills);
            }
            if (timeMills % logInterval == 0){
                reporter.update(network);
            }
        }

    }

    public TrafficGenerator getTrafficGenerator() {
        return trafficGenerator;
    }

    public void setTrafficGenerator(TrafficGenerator trafficGenerator) {
        this.trafficGenerator = trafficGenerator;
    }

    public TrafficStatus getTrafficStatus() {
        return trafficStatus;
    }

    public void setTrafficStatus(TrafficStatus trafficStatus) {
        this.trafficStatus = trafficStatus;
    }

    public NetworkScheduler getScheduler() {
        return scheduler;
    }

    public void setScheduler(NetworkScheduler scheduler) {
        this.scheduler = scheduler;
    }
}
