package resourcemanager.system.peer.rm;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.address.Address;
import se.sics.kompics.network.Network;
import se.sics.kompics.timer.SchedulePeriodicTimeout;
import se.sics.kompics.timer.ScheduleTimeout;
import se.sics.kompics.timer.Timer;
import se.sics.kompics.web.Web;
import simulator.snapshot.Snapshot;
import system.peer.RmPort;
import tman.system.peer.tman.TManSample;
import tman.system.peer.tman.TManSamplePort;
import common.configuration.RmConfiguration;
import common.peer.AvailableResources;
import common.simulation.RequestBatchResource;
import common.simulation.RequestResource;
import cyclon.system.peer.cyclon.CyclonSample;
import cyclon.system.peer.cyclon.CyclonSamplePort;
import cyclon.system.peer.cyclon.PeerDescriptor;

/**
 * Should have some comments here.
 *
 * @author jdowling
 */
public final class ResourceManager extends ComponentDefinition {

    private static final Logger logger = LoggerFactory.getLogger(ResourceManager.class);
    Positive<RmPort> indexPort = positive(RmPort.class);
    Positive<Network> networkPort = positive(Network.class);
    Positive<Timer> timerPort = positive(Timer.class);
    Negative<Web> webPort = negative(Web.class);
    Positive<CyclonSamplePort> cyclonSamplePort = positive(CyclonSamplePort.class);
    Positive<TManSamplePort> tmanPort = positive(TManSamplePort.class);
    ArrayList<Address> neighbours = new ArrayList<Address>();
    private Address self;
    private RmConfiguration configuration;
    Random random;
    private AvailableResources availableResources;
    Comparator<PeerDescriptor> peerAgeComparator = new Comparator<PeerDescriptor>() {
        @Override
        public int compare(PeerDescriptor t, PeerDescriptor t1) {
            if (t.getAge() > t1.getAge()) {
                return 1;
            } else {
                return -1;
            }
        }
    };
    /*
     * final int | number of probes
     * hashmap | key: id, value: event
     * hashmap | key: id, value: [probes]
     * FIFO linked list | job queued
     */
    
    // Number of probes sent to random workers
    private static final int NBPROBES = 2;
    
    // Jobs managed by the RM, waiting for probes to pick the best worker, indexed by the job ID
    private HashMap<Long, ManagedJob> managedJobs;
    
    // A hashmap storing probes infos associated with a managed job, indexed by the job id
    private HashMap<Long, LinkedList<ProbeInfos>> outstandingProbes;
    
    // FIFO list of jobs to be processed by the worker
    private LinkedList<Job> pendingJobs;
    
    // Set of jobs currently processed by the worker
    private LinkedList<Job> activeJobs;
    
    // For batch jobs, keeps the number of tasks 
    private HashMap<Long, Integer> batchJobs;
    
    // For batch jobs, keeps the amount of tasks that have been processed for each job
    private HashMap<Long, Integer> batchJobsProcessed;
    
    public HashMap<Long, Long> uniqueJobsWaitingTime;
    
	
    public ResourceManager() {

        subscribe(handleInit, control);
        subscribe(handleCyclonSample, cyclonSamplePort);
        subscribe(handleRequestResource, indexPort);
        subscribe(handleUpdateTimeout, timerPort);
        subscribe(handleResourceAllocationRequest, networkPort);
        subscribe(handleResourceAllocationResponse, networkPort);
        subscribe(handleResourceAllocationRelease, networkPort);
        subscribe(handleTManSample, tmanPort);
        subscribe(handleProbeRequest, networkPort);
        subscribe(handleProbeResponse, networkPort);
        subscribe(handleJobProcessingTimeout, timerPort);
        subscribe(handleRequestBatchResource, indexPort);
//        subscribe(handleMyTimeout, timerPort);
    }
	
    Handler<RmInit> handleInit = new Handler<RmInit>() {
        @Override
        public void handle(RmInit init) {
            self = init.getSelf();
            configuration = init.getConfiguration();
            random = new Random(init.getConfiguration().getSeed());
            availableResources = init.getAvailableResources();
            long period = configuration.getPeriod();
            availableResources = init.getAvailableResources();
            SchedulePeriodicTimeout rst = new SchedulePeriodicTimeout(period, period);
            rst.setTimeoutEvent(new UpdateTimeout(rst));
            trigger(rst, timerPort);
            managedJobs = new HashMap<Long, ManagedJob>();
            outstandingProbes = new HashMap<Long, LinkedList<ProbeInfos>>();
            pendingJobs = new LinkedList<Job>();
            activeJobs = new LinkedList<Job>();
            uniqueJobsWaitingTime = new HashMap<Long, Long>();
            batchJobs = new HashMap<Long, Integer>();
            batchJobsProcessed = new HashMap<Long, Integer>();
        }
    };


    Handler<UpdateTimeout> handleUpdateTimeout = new Handler<UpdateTimeout>() {
        @Override
        public void handle(UpdateTimeout event) {

            // pick a random neighbour to ask for index updates from. 
            // You can change this policy if you want to.
            // Maybe a gradient neighbour who is closer to the leader?
            if (neighbours.isEmpty()) {
                return;
            }
            Address dest = neighbours.get(random.nextInt(neighbours.size()));


        }
    };


    Handler<RequestResources.Request> handleResourceAllocationRequest = new Handler<RequestResources.Request>() {
        @Override
        public void handle(RequestResources.Request event) {
            // TODO 
        	/*
        	 * Receiving this means that this RM was the least loaded of the RMs probed
        	 * We add the event to the FIFO queue
        	 * Loop
        	 *	 	If there is enough available resources for the first job of the queue: 
        	 * 			pop the first job of the queue and add it to the activeJobs set
        	 * 			update available resources
        	 * 			send a ResourceAllocationResponse event
        	 */
        	Job job = new Job(event.getId(), event.getIdBatch(), event.getNumCpus(), event.getMemoryInMbs(), event.getSource());
        	pendingJobs.add(job);
//        	System.out.println(self + " : " + pendingJobs.size());
        	tryToProcessNewJob();
        }
    };
    Handler<RequestResources.Response> handleResourceAllocationResponse = new Handler<RequestResources.Response>() {
        @Override
        public void handle(RequestResources.Response event) {
            // TODO 
        	/*
        	 * Receiving this means we have been allocated resources for a job
        	 * We start a timeout for this job containing the id and the worker
        	 */
        	long id = event.getId();
        	
    		if(event.getIdBatch() != -1){
    			id = event.getIdBatch();
    			batchJobsProcessed.put(event.getIdBatch(), batchJobsProcessed.get(event.getIdBatch()) + 1);
    			if(batchJobsProcessed.get(event.getIdBatch()) == batchJobs.get(event.getIdBatch())){
//    				Snapshot.printInFile("bx" + id + "@" + System.currentTimeMillis() / 1000L);
    				batchJobs.remove(event.getIdBatch());
    				batchJobsProcessed.remove(event.getIdBatch());
    			}
    		}
    		else{
//    			Snapshot.printInFile("jx" + id + "@" + System.currentTimeMillis() / 1000L);
    			uniqueJobsWaitingTime.put(id, System.currentTimeMillis() / 1000L - uniqueJobsWaitingTime.get(id));
    			Long waited = System.currentTimeMillis() / 1000L - uniqueJobsWaitingTime.get(id);
    			uniqueJobsWaitingTime.remove(id);
    			Snapshot.printInFile(waited.toString());
    		}
        	int processingTime = managedJobs.get(id).getTimeToHoldResource();
        	managedJobs.remove(id);
        	ScheduleTimeout jobProcessingTimeout = new ScheduleTimeout(processingTime);
        	jobProcessingTimeout.setTimeoutEvent(new JobProcessingTimeout(jobProcessingTimeout, event.getId(), event.getSource()));
        	trigger(jobProcessingTimeout, timerPort);
        }
    };
    
    /*
     * handler resourceRelease
     * 		remove the job from the activeJobs set
     * 		update available resources
     * 		loop
     * 			If there is enough available resources for the first job of the queue: 
     * 				pop the first job of the queue and add it to the activeJobs set
     * 				update available resources
   	 * 				send a ResourceAllocationResponse event
     */
    
    Handler<RequestResources.Release> handleResourceAllocationRelease = new Handler<RequestResources.Release>(){
    	@Override
    	public void handle(RequestResources.Release event){
    		Job releasedJob = null;
    		int releasedJobIndex = -1;
    		for(int i=0; i < activeJobs.size(); i++){
    			if (activeJobs.get(i).getId() == event.getId()){
    				releasedJob = activeJobs.get(i);
    				releasedJobIndex = i;
    			}
    		}
    		if(releasedJob == null){
    			Snapshot.printInFile("Trying to remove a job not present in the activeJobs list");
    			System.exit(0);
    		}
    		else{
    			activeJobs.remove(releasedJobIndex);
    			availableResources.release(releasedJob.getNumCPUs(), releasedJob.getMemoryInMbs());
    			//Snapshot.printInFile("-" + releasedJob.getId() + "@" + System.currentTimeMillis() / 1000L);
    			tryToProcessNewJob();
    		}
    			
    	}
    };
    
    Handler<CyclonSample> handleCyclonSample = new Handler<CyclonSample>() {
        @Override
        public void handle(CyclonSample event) {
//            System.out.println("Received samples: " + event.getSample().size());
            
            // receive a new list of neighbours
            neighbours.clear();
            neighbours.addAll(event.getSample());

        }
    };
	
    Handler<RequestResource> handleRequestResource = new Handler<RequestResource>() {
        @Override
        public void handle(RequestResource event) {
            
//            System.out.println("Allocate resources: " + event.getNumCpus() + " + " + event.getMemoryInMbs());
            // TODO: 
            /*
             * We pick a fixed number of neighbours at random
             * We send each one a probe (with the id of the job) to check their loads
             */
            outstandingProbes.put(event.getId(), new LinkedList<ProbeInfos>());
            managedJobs.put(event.getId(), new ManagedJob(event.getId(), -1, event.getNumCpus(), event.getMemoryInMbs(), self, event.getTimeToHoldResource()));
//            Snapshot.printInFile("j+" + event.getId() + "@" + System.currentTimeMillis() / 1000L);
            uniqueJobsWaitingTime.put(event.getId(), System.currentTimeMillis() / 1000L);
            
            ArrayList<Address> randomList = pickAtRandom(NBPROBES);
            for(int i = 0; i < randomList.size(); i++){
            	Probe.Request probe = new Probe.Request(self, randomList.get(i), event.getId(), randomList.size());
            	trigger(probe, networkPort);
            }
            
        }
    };
    
    Handler<RequestBatchResource> handleRequestBatchResource = new Handler<RequestBatchResource>(){
    	@Override
    	public void handle(RequestBatchResource event){
    		if(neighbours.size() != 0){
	    		batchJobs.put(event.getId(), event.getNbNodes());
	    		batchJobsProcessed.put(event.getId(), 0);
	    		outstandingProbes.put(event.getId(), new LinkedList<ProbeInfos>());
	    		int nbNodes = event.getNbNodes();
	    		managedJobs.put(event.getId(), new ManagedJob(event.getId(), event.getId(), event.getNumCpus(), event.getMemoryInMbs(), self, event.getTimeToHoldResource()));
	    		Snapshot.printInFile("b+" + event.getId() + "@" + System.currentTimeMillis() / 1000L);
	    		ArrayList<Address> randomList = pickAtRandom(NBPROBES * nbNodes);
	    		for(int i = 0; i < randomList.size(); i++){
	            	Probe.Request probe = new Probe.Request(self, randomList.get(i), event.getId(), randomList.size());
	            	trigger(probe, networkPort);
	            }
    		}
    	}
    };
    /*
     * handler probeRequest
     * 		send back a probeResponse containing the id of the job and the number of jobs in the queue
     */
    Handler<Probe.Request> handleProbeRequest = new Handler<Probe.Request>(){
    	@Override
    	public void handle(Probe.Request event) {
    		int nbPendingJobs = activeJobs.size() + pendingJobs.size();
    		Probe.Response res = new Probe.Response(self, event.getSource(), event.getId(), nbPendingJobs, event.getNbProbes());
    		trigger(res, networkPort);
    	}
    };
    
    /*
     * handler probeResponse
     * 		add the probe into the hashmap
     * 		if we received a fixed number of probes
     * 			pick the least loaded worker
     * 			retreive the job in the jobs hashtable
     * 			send a resourceAllocationRequest to this worker 
     */
    Handler<Probe.Response> handleProbeResponse = new Handler<Probe.Response>(){
    	@Override
    	public void handle(Probe.Response event) {
    		ProbeInfos probeInfos = new ProbeInfos(event.getSource(), event.getNbPendingJobs(), event.getNbProbes());
    		outstandingProbes.get(event.getId()).add(probeInfos);
    		if (outstandingProbes.get(event.getId()).size() == event.getNbProbes()){
    			if(batchJobs.containsKey(event.getId())){
    				int nbNodes = batchJobs.get(event.getId());
    				LinkedList<Address> bestMWorkers = pickBestMWorkers(outstandingProbes.get(event.getId()), nbNodes);
    				ManagedJob managedJob = managedJobs.get(event.getId());
    				for (int i = 0; i < nbNodes; i++){
    					RequestResources.Request req = new RequestResources.Request(self, bestMWorkers.get(i), managedJob.getNumCPUs(), managedJob.getMemoryInMbs(), managedJob.getId() + i + 1, managedJob.getId());
    					trigger(req, networkPort);
    				}
    			}
    			else{
	    			Address bestWorker = pickBestWorker(outstandingProbes.get(event.getId()));
	    			ManagedJob managedJob = managedJobs.get(event.getId());
	    			RequestResources.Request req = new RequestResources.Request(self, bestWorker, managedJob.getNumCPUs(), managedJob.getMemoryInMbs(), managedJob.getId(), -1);
	    			trigger(req, networkPort);
    			}
    			
    			// We can remove the job from the outstandingProbes list
    			outstandingProbes.remove(event.getId());
    		}
    	}
    };
    
    
    /*
     * handler jobTimeout
     * 		send a resourceRelease to the worker with the id of the job
     */
    
    Handler<JobProcessingTimeout> handleJobProcessingTimeout = new Handler<JobProcessingTimeout>(){
    	@Override
    	public void handle(JobProcessingTimeout event){
    		RequestResources.Release rel = new RequestResources.Release(self, event.getWorker(), event.getId());
    		trigger(rel, networkPort);
    	}
    };
    
    Handler<TManSample> handleTManSample = new Handler<TManSample>() {
        @Override
        public void handle(TManSample event) {
            // TODO: 
        }
    };
    
    /*
     * loop
     *		If there is enough available resources for the first job of the queue: 
     * 			pop the first job of the queue and add it to the activeJobs set
     * 			update available resources
  	 * 			send a ResourceAllocationResponse event
     */
    
    public void tryToProcessNewJob(){
    	if (pendingJobs.size() != 0){
    		Job firstJob = pendingJobs.getFirst();
	    	if (availableResources.getNumFreeCpus() >= firstJob.getNumCPUs() && availableResources.getFreeMemInMbs() >= firstJob.getMemoryInMbs()){
	    		availableResources.allocate(firstJob.getNumCPUs(), firstJob.getMemoryInMbs());
	    		pendingJobs.removeFirst();
//	    		System.out.println(self + " : " + pendingJobs.size());
	    		activeJobs.add(firstJob);
	    		RequestResources.Response res = new RequestResources.Response(self, firstJob.getInitiator(), true, firstJob.getId(), firstJob.getIdBatch());
	    		trigger(res, networkPort);
	    		tryToProcessNewJob();
	    	}
    	}
    }
    
    /*
     * When receiving a Probe.Response message, its informations are store in a ProbeInfos instance
     * Then, this instance is added to the outstandingProbes hashmap for the right id
     */
    private class ProbeInfos{
    	private final Address worker;
    	private final int nbPendingJobs;
    	private final int nbProbes;
    	
    	public ProbeInfos(Address worker, int nbPendingJobs, int nbProbes){
    		this.worker = worker;
    		this.nbPendingJobs = nbPendingJobs;
    		this.nbProbes = nbProbes;
    	}
    	
    	public int getNbProbes() {
			return nbProbes;
		}

		public Address getWorker(){
    		return worker;
    	}
    	
    	public int getNbPendingJobs(){
    		return nbPendingJobs;
    	}
    }
    
    /*
     * When receiving a RequestResources.Request message, its informations are stored in a Job instance
     * Then, this instance is added to the pendingJobs linked list until resources are available
     * When resources are available, this instance is popped from the linked list and added to the activeJobs set
     * Upon reception of the appropriate RequestResources.Release message, it is removed from the activeJobs set 
     */
    private class Job{
    	private final Address initiator;
    	private final long idBatch;
    	private final long id;
    	private final int numCPUs;
    	private final int amountMemInMb;
    	
    	public Job(long id, long idBatch, int numCPUs, int amountMemInMb, Address initiator){
    		this.id = id;
    		this.idBatch = idBatch;
    		this.numCPUs = numCPUs;
    		this.amountMemInMb = amountMemInMb;
    		this.initiator = initiator;
    	}

		public long getIdBatch() {
			return idBatch;
		}

		public Address getInitiator() {
			return initiator;
		}

		public long getId() {
			return id;
		}

		public int getNumCPUs() {
			return numCPUs;
		}

		public int getMemoryInMbs() {
			return amountMemInMb;
		}
    	
    	
    }
    
    private class ManagedJob extends Job{
    	private final int timeToHoldResource;
    	
    	public ManagedJob(long id, long idBatch, int numCPUs, int amountMemInMb, Address initiator, int timeToHoldResource){
    		super(id, idBatch, numCPUs, amountMemInMb, initiator);
    		this.timeToHoldResource = timeToHoldResource;
    	}

		public int getTimeToHoldResource() {
			return timeToHoldResource;
		}
    	
    	
    }
    
    private ArrayList<Address> pickAtRandom(int maxProbes){
    	ArrayList<Integer> intList = new ArrayList<Integer>(neighbours.size() + 1);
    	for(int i = 0; i < neighbours.size() + 1; i++){
    		intList.add(i);
    	}
    	Collections.shuffle(intList);
    	
    	// If the number of nodes is < to the fixed number of probes use less probes
    	int nbProbes = 0;
    	if (neighbours.size() + 1 >= maxProbes){
    		nbProbes = maxProbes;
    	}
    	else{
    		nbProbes = neighbours.size() + 1;
    	}
    	
    	ArrayList<Address> randomList = new ArrayList<Address>(nbProbes);
    	for(int i = 0; i < nbProbes; i++){
    		if (intList.get(i) == neighbours.size()){
    			randomList.add(self);
    		}
    		else{
    			randomList.add(neighbours.get(intList.get(i)));
    		}
    	}
    	return randomList;
    }
    
    private Address pickBestWorker(LinkedList<ProbeInfos> probeInfosList){
    	int indexMin = 0;
    	int min = Integer.MAX_VALUE;
    	
    	for (int i = 0; i < probeInfosList.size(); i++){
    		if(probeInfosList.get(i).getNbPendingJobs() < min){
    			indexMin = i;
    			min = probeInfosList.get(i).getNbPendingJobs();
    		}
    	}
    	
    	return probeInfosList.get(indexMin).getWorker();
    }
    
    private LinkedList<Address> pickBestMWorkers(LinkedList<ProbeInfos> probeInfosList, int nbDesiredNodes){
    	LinkedList<Address> bestMWorkers = new LinkedList<Address>();
    	int indexMin = 0;
    	int min = Integer.MAX_VALUE;
    	
    	// If a RM has not enough neighbours reduce the number of workers
    	if(neighbours.size() < nbDesiredNodes){
    		nbDesiredNodes = neighbours.size();
    	}
    	
    	for(int j = 0; j < nbDesiredNodes; j++){
    		min = Integer.MAX_VALUE;
	    	for (int i = 0; i < probeInfosList.size(); i++){
	    		if(probeInfosList.get(i).getNbPendingJobs() < min){
	    			indexMin = i;
	    			min = probeInfosList.get(i).getNbPendingJobs();
	    		}
	    	}
	    	bestMWorkers.add(probeInfosList.get(indexMin).getWorker());
	    	probeInfosList.remove(indexMin);
    	}
    	return bestMWorkers;
    }
}