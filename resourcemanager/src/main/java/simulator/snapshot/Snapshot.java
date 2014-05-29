package simulator.snapshot;

import common.peer.AvailableResources;
import common.peer.ResourceType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import se.sics.kompics.address.Address;

public class Snapshot {

    private static ConcurrentHashMap<Address, PeerInfo> peers = 
            new ConcurrentHashMap<Address, PeerInfo>();
    private static ConcurrentHashMap<Long, Long> batches =
            new ConcurrentHashMap<Long, Long>();
    private static ConcurrentHashMap<Long, Long> jobs =
            new ConcurrentHashMap<Long, Long>();
    private static List<Long> waitingTimes, batchWaitingTimes, totalTimes;
    private static int counter = 0;
    private static String FILENAME = "search.out";

    static {
        waitingTimes = Collections.synchronizedList(new LinkedList<Long>());
        batchWaitingTimes = Collections.synchronizedList(new LinkedList<Long>());
        totalTimes = Collections.synchronizedList(new LinkedList<Long>());
    }

    public static void init(int numOfStripes) {
        FileIO.write("", FILENAME);
    }


    public static void addPeer(Address address, AvailableResources availableResources) {
        printInFile(address.getId()+" ("+counter+"): *** joining ***");
        peers.put(address, new PeerInfo(availableResources));
    }

    public static void removePeer(Address address) {
        printInFile(address.getId()+" ("+counter+"): *** crashing ***");
        peers.remove(address);
    }

    public static void updateNeighbours(Address address, ArrayList<Address> partners) {
        PeerInfo peerInfo = peers.get(address);

        if (peerInfo == null) {
            return;
        }

        peerInfo.setNeighbours(partners);
    }
    
    public static void nodeReport(Address self, int pendingJobs, int activeJobs, int cpu, int memory) {
    }
    
    public static void cyclonSampleReceived(Address self, int size) {
        printInFile(self.getId()+" ("+counter+"): Received cyclon sample of size "+size);
    }
    
    public static void tmanSampleReceived(Address self, ResourceType type, int size) {
        printInFile(self.getId()+" ("+counter+"): Received tman sample "+size+" for resource "+type);
    }
    
    public static void resourceDemand(Address self, int cpus, int memory, long jobId) {
        jobs.put(jobId, System.currentTimeMillis()/1000L);
        printInFile(self.getId()+" ("+counter+"): job requested ("+cpus+"C, "+memory+"MB) - id "+jobId);
    }
    
    public static void resourceBatchDemand(Address self, int cpus, int memory, int nodes, long batchId) {
        batches.put(batchId, System.currentTimeMillis()/1000L);
        printInFile(self.getId()+" ("+counter+"): batch requested ("+nodes+"N, "+cpus+"C, "+memory+"MB) - id "+batchId);
    }
    
    public static void probeRequested(Address self, int activeJobs, int pendingJobs) {
        printInFile(self.getId()+" ("+counter+"): probe request ("+activeJobs+"A + "+pendingJobs+"P)");
    }
    public static void probeResponded(Address self) {
        printInFile(self.getId()+" ("+counter+"): probe responded");
    }
    
    public static void jobTimeout(Address self, Address worker, long jobId) {
        long executionTime = System.currentTimeMillis()/1000L - jobs.remove(jobId);
        totalTimes.add(executionTime);
        printInFile(self.getId()+" ("+counter+"): job "+jobId+" completed on "+worker.getId() + " (total time: "+executionTime+" ms)");
    }
    
    public static void startingJob(Address self, long jobId, long batchId, int pendingJobs, int remainingCpu, int remainingMemory) {
        printInFile(self.getId()+" ("+counter+"): job "+batchId+"-"+jobId+" started - pending "+pendingJobs);
    }
    
    public static void releaseJob(Address self, long jobId) {
        printInFile(self.getId()+" ("+counter+"): job "+jobId+" released");
    }
    
    public static void allocateJob(Address self, long jobId, long waited) {
        long waitingTime = System.currentTimeMillis()/1000L - jobs.get(jobId);
        waitingTimes.add(waitingTime);
        printInFile(self.getId()+" ("+counter+"): job "+jobId+" allocated - waited "+waitingTime+" ms");
    }
    
    public static void allocateJobInBatch(Address self, long batchId, long jobId) {
        long waitingTime = System.currentTimeMillis()/1000L - jobs.get(jobId);
        batchWaitingTimes.add(waitingTime);
        printInFile(self.getId()+" ("+counter+"): job "+batchId+"-"+jobId+" allocated");
    }
    
    public static void allJobsInBatchAllocated(Address self, long batchId) {
        printInFile(self.getId()+" ("+counter+"): all jobs of batch "+batchId+" allocated");
    }
    
    public static void allocationRequest(Address self, long jobId, long batchId, Address source, int pending) {
        printInFile(self.getId()+" ("+counter+"): being allocated job "+batchId+"-"+jobId+" from "+source.getId()+" - pending jobs "+pending);
    }

    public static void printInFile(String str){
    	System.out.println(str);
//    	FileIO.append(str + ",", FILENAME);
    }

    public static void report() {
        String str = prepareReport();
        //System.out.println(str);
        FileIO.append(str, FILENAME);
    }
    
    public static void finalReport() {
        String str = prepareReport();
        
        long[] waitingTimeStats = computeStatistics(waitingTimes);
        long[] batchWaitingTimeStats = computeStatistics(batchWaitingTimes);
        long[] executionTimeStats = computeStatistics(totalTimes);
        
        str += "";
        str += String.format("%1$-18s: %3$7s | %4$7s | %5$7s | %6$7s | %7$7s | %2$7s\n",
                "** Times **", "#jobs", "Min", "Max",
                "Mean", "p05", "p95");
        str += String.format("%1$-18s: %3$7s | %4$7s | %5$7s | %6$7s | %7$7s | %2$7s\n",
                "Waiting time", waitingTimeStats[0], waitingTimeStats[1], waitingTimeStats[2],
                waitingTimeStats[3], waitingTimeStats[4], waitingTimeStats[5]);
        str += String.format("%1$-18s: %3$7s | %4$7s | %5$7s | %6$7s | %7$7s | %2$7s\n",
                "Batch waiting time", batchWaitingTimeStats[0], batchWaitingTimeStats[1], batchWaitingTimeStats[2],
                batchWaitingTimeStats[3], batchWaitingTimeStats[4], batchWaitingTimeStats[5]);
        str += String.format("%1$-18s: %3$7s | %4$7s | %5$7s | %6$7s | %7$7s | %2$7s\n",
                "Execution time", executionTimeStats[0], executionTimeStats[1], executionTimeStats[2],
                executionTimeStats[3], executionTimeStats[4], executionTimeStats[5]);
        str += "";
        
        System.out.println(str);
        FileIO.append(str, FILENAME);
    }
    
    private static String prepareReport() {
        String str = new String();
        str += "current time: " + counter++ + "\n";
        str += reportNetworkState();
        str += reportDetails();
        str += "###\n";
        return str;
    }

    private static String reportNetworkState() {
        String str = "---\n";
        int totalNumOfPeers = peers.size();
        str += "total number of peers: " + totalNumOfPeers + "\n";

        return str;
    }


    private static String reportDetails() {
        String str = "---\n";
        int maxFreeCpus = 0;
        int minFreeCpus = Integer.MAX_VALUE;
        int maxFreeMemInMb = 0;
        int minFreeMemInMb = Integer.MAX_VALUE;
        for (PeerInfo p : peers.values()) {
            if (p.getNumFreeCpus() > maxFreeCpus) {
                maxFreeCpus = p.getNumFreeCpus();
            }
            if (p.getNumFreeCpus() < minFreeCpus) {
                minFreeCpus = p.getNumFreeCpus();
            }
            if (p.getFreeMemInMbs() > maxFreeMemInMb) {
                maxFreeMemInMb = p.getFreeMemInMbs();
            }
            if (p.getFreeMemInMbs() < minFreeMemInMb) {
                minFreeMemInMb = p.getFreeMemInMbs();
            }
        }
        str += "Peer with max num of free cpus: " + maxFreeCpus + "\n";
        str += "Peer with min num of free cpus: " + minFreeCpus + "\n";
        str += "Peer with max amount of free mem in MB: " + maxFreeMemInMb + "\n";
        str += "Peer with min amount of free mem in MB: " + minFreeMemInMb + "\n";
        
        return str;
    }
    
    static private long[] computeStatistics(List<Long> times) {
        Collections.sort(times);
        int size = times.size();
        
        long min = 0, max = 0, mean = 0, p5 = 0, p95 = 0, sum = 0;
        if (!times.isEmpty()) {
            min = Collections.min(times);
            max = Collections.max(times);
            
            for (Long t : times) {
                sum += t;
            }
            mean = sum / size;
            
            // 5th percentile
            int j5 = (int) Math.ceil(size * 0.05) - 1;
            p5 = times.get(j5);
            
            // 95th percentile
            int j95 = (int) Math.ceil(size * 0.95) - 1;
            p95 = times.get(j95);
        }
        
        long[] result = {(long)size, min, max, mean, p5, p95};
        return result;
    }
}
