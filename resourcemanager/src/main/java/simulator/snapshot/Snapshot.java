package simulator.snapshot;

import common.peer.AvailableResources;
import common.peer.ResourceType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import se.sics.kompics.address.Address;

public class Snapshot {

    private final static ConcurrentHashMap<Address, PeerInfo> peers;
    private final static ConcurrentHashMap<Long, Long> batches, jobs;
    private final static List<Long> waitingTimes, batchWaitingTimes, totalTimes;
    private static int counter = 0, totalJobs = 0;
    private final static String FILENAME = "search.out";

    static {
        peers = new ConcurrentHashMap<Address, PeerInfo>();
        jobs = new ConcurrentHashMap<Long, Long>();
        batches = new ConcurrentHashMap<Long, Long>();
        
        waitingTimes = Collections.synchronizedList(new LinkedList<Long>());
        batchWaitingTimes = Collections.synchronizedList(new LinkedList<Long>());
        totalTimes = Collections.synchronizedList(new LinkedList<Long>());
    }

    public static void init(int numOfStripes) {
        FileIO.write("", FILENAME);
    }


    public static void addPeer(Address address, AvailableResources availableResources) {
//        printInFile(address.getId()+" ("+counter+"): *** joining ***");
        peers.put(address, new PeerInfo(availableResources));
    }

    public static void removePeer(Address address) {
//        printInFile(address.getId()+" ("+counter+"): *** crashing ***");
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
//        printInFile(self.getId()+" ("+counter+"): Received cyclon sample of size "+size);
    }
    
    public static void tmanSampleReceived(Address self, ResourceType type, int size) {
//        printInFile(self.getId()+" ("+counter+"): Received tman sample of size "+size+" for resource "+type);
    }
    
    public static void resourceDemand(Address self, int cpus, int memory, long jobId) {
        long requestTime = System.currentTimeMillis()/1000L;
        totalJobs++;
        jobs.put(jobId, requestTime);
        printInFile(self.getId()+" ("+counter+"): job requested ("+cpus+"C, "+memory+"MB) - id "+jobId);
    }
    
    public static void resourceBatchDemand(Address self, int cpus, int memory, int nodes, long batchId) {
        batches.put(batchId, System.currentTimeMillis()/1000L);
        printInFile(self.getId()+" ("+counter+"): batch requested ("+nodes+"N, "+cpus+"C, "+memory+"MB) - id "+batchId);
    }
    
    public static void probeRequested(Address self, long jobId, int activeJobs, int pendingJobs, ResourceType type, int hops) {
//        printInFile(self.getId()+" ("+counter+"): probe "+jobId+" request ("+activeJobs+"A + "+pendingJobs+"P) for "+type+" and "+hops+" hops");
    }
    public static void probeResponded(Address self, Address source, long jobId) {
//        printInFile(self.getId()+" ("+counter+"): probe "+jobId+" responded by "+source.getId());
    }
    public static void probeCanceled(Address self, long jobId) {
//        printInFile(self.getId()+" ("+counter+"): probe "+jobId+" canceled");
    }
    
    public static void jobTimeout(Address self, Address worker, long jobId) {
        long executionTime = System.currentTimeMillis()/1000L - jobs.remove(jobId);
        totalTimes.add(executionTime);
        printInFile(self.getId()+" ("+counter+"): job "+jobId+" completed on "+worker.getId() + " (total time: "+executionTime+" ms)");
    }
    
    public static void startingJob(Address self, long jobId, long batchId, int pendingJobs, int remainingCpu, int remainingMemory) {
//        printInFile(self.getId()+" ("+counter+"): job "+batchId+"-"+jobId+" started - pending "+pendingJobs);
    }
    
    public static void releaseJob(Address self, long jobId) {
//        printInFile(self.getId()+" ("+counter+"): job "+jobId+" released");
    }
    
    public static void allocateJob(Address self, long jobId) {
        long waitingTime = System.currentTimeMillis()/1000L - jobs.get(jobId);
        waitingTimes.add(waitingTime);
//        printInFile(self.getId()+" ("+counter+"): job "+jobId+" allocated - waited "+waitingTime+" ms");
    }
    
    public static void allocateJobInBatch(Address self, long batchId, long jobId) {
        long waitingTime = System.currentTimeMillis()/1000L - jobs.get(jobId);
        batchWaitingTimes.add(waitingTime);
//        printInFile(self.getId()+" ("+counter+"): job "+batchId+"-"+jobId+" allocated");
    }
    
    public static void allJobsInBatchAllocated(Address self, long batchId) {
//        printInFile(self.getId()+" ("+counter+"): all jobs of batch "+batchId+" allocated");
    }

    public static void printInFile(String str){
    	System.out.println(str);
//    	FileIO.append(str + ",", FILENAME);
    }

    public static void report() {
        String str = prepareReport(false);
        
        if (counter % 100 == 0) {
            System.out.println(str);
        }
        
//        System.out.println(str);
//        FileIO.append(str, FILENAME);
//        if (counter > 25) {
//            finalReport();
//            System.exit(2);
//        }
    }
    
    public static void finalReport() {
        String str = prepareReport(true);
        
        HashMap<String, List<Long>> times = new HashMap<String, List<Long>>();
        times.put("Waiting time", waitingTimes);
        times.put("Batch waiting time", batchWaitingTimes);
        times.put("Execution time", totalTimes);
        
        str += "\nTotal requested jobs: "+totalJobs+"\n";
        str += String.format("%1$-18s: %3$7s | %4$7s | %5$7s | %6$7s | %7$7s | %2$7s\n",
                "** Times **", "#jobs", "Min", "Max", "Mean", "p05", "p95");
        for (String key : times.keySet()) {
            long[] stats = computeStatistics(times.get(key));
            str += String.format("%1$-18s: %3$7s | %4$7s | %5$7s | %6$7s | %7$7s | %2$7s\n",
                    key, stats[0], stats[1], stats[2], stats[3], stats[4], stats[5]);
        }
        str += "###";
        
        System.out.println(str);
        FileIO.append(str, FILENAME);
    }
    
    private static String prepareReport(boolean details) {
        String str = new String();
        str += "current time: " + counter++ + "\n";
        str += reportNetworkState(details);
        str += reportDetails(details);
        str += "###\n";
        return str;
    }

    private static String reportNetworkState(boolean details) {
        String str = "---\n";
        int totalNumOfPeers = peers.size();
        str += "total number of peers: " + totalNumOfPeers + "\n";
        if (details) {
            for (Address p : peers.keySet()) {
                PeerInfo info = peers.get(p);
                str += String.format("%1$s (%2$sC %3$sM) - ", p.getId(), info.getNumFreeCpus(), info.getFreeMemInMbs());
                for (Address q : info.getNeighbours()) {
                    str += q.getId() + ", ";
                }
                str += "\n";
            }
            str += "\n";
        }

        return str;
    }


    private static String reportDetails(boolean details) {
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
        str += "Free cpus (min/max): " + minFreeCpus + "/" + maxFreeCpus + "\n";
        str += "Free mem in MB (min/max): " + minFreeMemInMb + "/" + maxFreeMemInMb + "\n";
        
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
