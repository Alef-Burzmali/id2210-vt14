package resourcemanager.system.peer.rm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
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
import common.peer.ResourceType;
import common.simulation.RequestBatchResource;
import common.simulation.RequestResource;
import cyclon.system.peer.cyclon.CyclonSample;
import cyclon.system.peer.cyclon.CyclonSamplePort;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;

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
    EnumMap<ResourceType, ArrayList<Address>> neighbours
            = new EnumMap<ResourceType, ArrayList<Address>>(ResourceType.class);
    private Address self;
    private RmConfiguration configuration;
    Random random;
    private AvailableResources availableResources;

    /**
     * Number of probes sent to random workers
     */
    private static final int NBPROBES = 2;

    /**
     * Jobs managed by the RM, waiting for probes to pick the best worker, indexed by the job ID
     */
    private HashMap<Long, ManagedJob> waitingJobs;
    
    /**
     * Workers that have acknowledged our request and are waiting to process it.
     */
    private HashMap<Long, HashSet<Address>> outstandingProbes;
    
    /**
     * FIFO list of jobs to be processed by the worker, with late binding
     */
    private LinkedList<Job> pendingJobs;

    /**
     * Set of jobs currently processed by the worker
     */
    private LinkedList<Job> activeJobs;
    
    /**
     * List of workers that have already answered our request for a batch.
     */
    private HashMap<Long, HashSet<Address>> workersForBatch;

    /**
     * Select if we are using TMan (and gradient) or Cyclon (and random search)
     */
    private boolean useGradient;

    
    public ResourceManager() {
        subscribe(handleInit, control);
        subscribe(handleCyclonSample, cyclonSamplePort);
        subscribe(handleRequestResource, indexPort);
        subscribe(handleUpdateTimeout, timerPort);
        subscribe(handleResourceAllocationResponse, networkPort);
        subscribe(handleResourceAllocationRelease, networkPort);
        subscribe(handleTManSample, tmanPort);
        subscribe(handleProbeRequest, networkPort);
        subscribe(handleProbeResponse, networkPort);
        subscribe(handleProbeCancel, networkPort);
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
            SchedulePeriodicTimeout rst = new SchedulePeriodicTimeout(period, period);
            rst.setTimeoutEvent(new UpdateTimeout(rst));
            trigger(rst, timerPort);

            waitingJobs = new HashMap<Long, ManagedJob>();
            outstandingProbes = new HashMap<Long, HashSet<Address>>();
            pendingJobs = new LinkedList<Job>();
            activeJobs = new LinkedList<Job>();
            workersForBatch = new HashMap<Long, HashSet<Address>>();

            useGradient = configuration.isGradient();
            if (useGradient) {
                for (ResourceType type : ResourceType.values()) {
                    neighbours.put(type, new ArrayList<Address>());
                }
            } else {
                ArrayList<Address> commonList = new ArrayList<Address>();
                for (ResourceType type : ResourceType.values()) {
                    neighbours.put(type, commonList);
                }
            }
        }
    };

    Handler<UpdateTimeout> handleUpdateTimeout = new Handler<UpdateTimeout>() {
        @Override
        public void handle(UpdateTimeout event) {
            // pick a random neighbour to ask for index updates from. 
            // You can change this policy if you want to.
            // Maybe a gradient neighbour who is closer to the leader?
//            if (neighbours.isEmpty()) {
//                return;
//            }
//            Address dest = neighbours.get(random.nextInt(neighbours.size()));

            Snapshot.nodeReport(self, activeJobs.size(), pendingJobs.size(), availableResources.getNumFreeCpus(), availableResources.getFreeMemInMbs());
        }
    };

    Handler<RequestResources.Response> handleResourceAllocationResponse = new Handler<RequestResources.Response>() {
        @Override
        public void handle(RequestResources.Response event) {
            /*
             * Receiving this means we have been allocated resources for a job
             * We start a timeout for this job containing the id and the worker
             */
            long jobId = event.getId();
            
            if (!waitingJobs.containsKey(jobId)) {
                RequestResources.Release rel = new RequestResources.Release(self, event.getSource(), event.getId());
                trigger(rel, networkPort);
            } else {
                ManagedJob job = waitingJobs.get(jobId);
                
                HashSet<Address> probes = outstandingProbes.get(jobId);
                probes.remove(event.getSource());
                
                if (!workersForBatch.containsKey(jobId)) {
                    workersForBatch.put(jobId, new HashSet<Address>());
                }
                HashSet<Address> workers = workersForBatch.get(jobId);
                workers.add(event.getSource());
                
                if (job.getJobsInBatch() <= workers.size()) {
                    // ok, job has begun
                    waitingJobs.remove(jobId);                    
                    Snapshot.allocateJob(self, jobId, job.getJobsInBatch());
                    
                    // cancel the remaining probes for this job
                    for (Address p : probes) {
                        trigger(new Probe.Cancel(self, p, jobId, job), networkPort);
                    }
                    outstandingProbes.remove(jobId);
                    
                    // prepare the timeout for our workers
                    int processingTime = job.getTimeToHoldResource();
                    for (Address w : workers) {
                        ScheduleTimeout jobProcessingTimeout = new ScheduleTimeout(processingTime);
                        jobProcessingTimeout.setTimeoutEvent(new JobProcessingTimeout(jobProcessingTimeout, jobId, w, job.getJobsInBatch()));
                        trigger(jobProcessingTimeout, timerPort);
                    }
                    workersForBatch.remove(jobId);
                }
            }
        }
    };

    /*
     * handler resourceRelease
     * 		remove the job from the activeJobs set
     * 		update available resources
     */
    Handler<RequestResources.Release> handleResourceAllocationRelease = new Handler<RequestResources.Release>() {
        @Override
        public void handle(RequestResources.Release event) {
            for (Job job : activeJobs) {
                if (job.getId() == event.getId()) {
                    activeJobs.remove(job);
                    availableResources.release(job.getNumCPUs(), job.getMemoryInMbs());
                    Snapshot.releaseJob(self, job.getId());
                    break;
                }
            }
            
            tryToProcessNewJob();
        }
    };

    Handler<CyclonSample> handleCyclonSample = new Handler<CyclonSample>() {
        @Override
        public void handle(CyclonSample event) {
            if (useGradient) {
                return; // we don't care
            }

            // receive a new list of neighbours
            // all the lists in neighbours are the same object, only replace one of them
            // it will propagate to all
            neighbours.get(ResourceType.CPU).clear();
            neighbours.get(ResourceType.CPU).addAll(event.getSample());
            
            Snapshot.cyclonSampleReceived(self, event.getSample().size());
            Snapshot.updateNeighbours(self, neighbours.get(ResourceType.CPU));
        }
    };

    Handler<TManSample> handleTManSample = new Handler<TManSample>() {
        @Override
        public void handle(TManSample event) {
            if (!useGradient) {
                return; // we don't care
            }

            // receive a new list of neighbours
            ResourceType type = event.getType();
            neighbours.get(type).clear();
            neighbours.get(type).addAll(event.getSample());
            
            Snapshot.tmanSampleReceived(self, event.getType(), event.getSample().size());
            Snapshot.updateNeighbours(self, neighbours.get(ResourceType.CPU));
        }
    };

    Handler<RequestResource> handleRequestResource = new Handler<RequestResource>() {
        @Override
        public void handle(RequestResource event) {
            Snapshot.resourceDemand(self, event.getNumCpus(), event.getMemoryInMbs(), event.getId());

            /*
             * We pick a fixed number of neighbours at random
             * We send each one a probe (with the id of the job) to check their loads
             */
            ManagedJob job = new ManagedJob(event.getId(), 1, event.getNumCpus(), event.getMemoryInMbs(), self, event.getTimeToHoldResource());
            waitingJobs.put(event.getId(), job);
            outstandingProbes.put(event.getId(), new HashSet<Address>());

            ResourceType requestedType = job.getResourceType();
            ArrayList<Address> sendProbesTo = selectProbes(NBPROBES, neighbours.get(requestedType));
            
            for (Address p : sendProbesTo) {
                Probe.Request probe;
                if (useGradient) {
                    probe = new Probe.Request(self, p, event.getId(), sendProbesTo.size(), 2, job);
                } else {
                    probe = new Probe.Request(self, p, event.getId(), sendProbesTo.size(), 0, job);
                }
                trigger(probe, networkPort);
            }
        }
    };

    Handler<RequestBatchResource> handleRequestBatchResource = new Handler<RequestBatchResource>() {
        @Override
        public void handle(RequestBatchResource event) {
            Snapshot.resourceBatchDemand(self, event.getNumCpus(), event.getMemoryInMbs(), event.getNbNodes(), event.getId());

            int nbNodes = event.getNbNodes();
            ManagedJob job = new ManagedJob(event.getId(), event.getNbNodes(), event.getNumCpus(), event.getMemoryInMbs(), self, event.getTimeToHoldResource());
            waitingJobs.put(event.getId(), job);
            outstandingProbes.put(event.getId(), new HashSet<Address>());

            ResourceType requestedType = job.getResourceType();
            ArrayList<Address> sendProbesTo = selectProbes(NBPROBES * nbNodes, neighbours.get(requestedType));

            for (Address p : sendProbesTo) {
                Probe.Request probe;
                if (useGradient) {
                    probe = new Probe.Request(self, p, event.getId(), sendProbesTo.size(), 2, job);
                } else {
                    probe = new Probe.Request(self, p, event.getId(), sendProbesTo.size(), 0, job);
                }
                trigger(probe, networkPort);
            }
        }
    };
    /*
     * handler probeRequest
     * 		We store the job in our pending queue for late binding
     *          and we forward the probe if requested.
     */
    Handler<Probe.Request> handleProbeRequest = new Handler<Probe.Request>() {
        @Override
        public void handle(Probe.Request event) {
            Snapshot.probeRequested(self, event.getId(), activeJobs.size(), pendingJobs.size(), event.getJob().getResourceType(), event.getNbHops());
            
            if (pendingJobs.contains(event.getJob()) || activeJobs.contains(event.getJob())) {
                return;
            }
            
            pendingJobs.add(event.getJob());
            Probe.Response response = new Probe.Response(event);
            trigger(response, networkPort);
            
            if (event.getNbHops() > 0 && !event.getSource().equals(self)) {
                ResourceType requestedType = event.getJob().getResourceType();
                ArrayList<Address> sendProbesTo = selectProbes(NBPROBES, neighbours.get(requestedType));
                
                for (Address p : sendProbesTo) {
                    if (!p.equals(self)) {
                        Probe.Request probe = new Probe.Request(event, p);
                        trigger(probe, networkPort);
                    }
                }
            }
            
            tryToProcessNewJob();
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
    Handler<Probe.Response> handleProbeResponse = new Handler<Probe.Response>() {
        @Override
        public void handle(Probe.Response event) {
            Snapshot.probeResponded(self, event.getSource(), event.getId());
            if (!outstandingProbes.containsKey(event.getId())) {
                Probe.Cancel cancel = new Probe.Cancel(event);
                trigger(cancel, networkPort);
                return;
            }
            
            outstandingProbes.get(event.getId()).add(event.getSource());
        }
    };
    
    Handler<Probe.Cancel> handleProbeCancel = new Handler<Probe.Cancel>() {
        @Override
        public void handle(Probe.Cancel event) {
            Snapshot.probeCanceled(self, event.getId());
            
            Job canceledJob = event.getJob();
            if (pendingJobs.contains(canceledJob)) {
                pendingJobs.remove(canceledJob);
            } else if (activeJobs.contains(canceledJob)) {
                activeJobs.remove(canceledJob);
                availableResources.release(canceledJob.getNumCPUs(), canceledJob.getMemoryInMbs());
            }
        }
    };

    /*
     * handler jobTimeout
     * 		send a resourceRelease to the worker with the id of the job
     */
    Handler<JobProcessingTimeout> handleJobProcessingTimeout = new Handler<JobProcessingTimeout>() {
        @Override
        public void handle(JobProcessingTimeout event) {
            Snapshot.jobTimeout(self, event.getWorker(), event.getId(), event.getJobsInBatch());

            RequestResources.Release rel = new RequestResources.Release(self, event.getWorker(), event.getId());
            trigger(rel, networkPort);
        }
    };

    /*
     * loop
     *		If there is enough available resources for the first job of the queue: 
     * 			pop the first job of the queue and add it to the activeJobs set
     * 			update available resources
     * 			send a ResourceAllocationResponse event
     */
    public synchronized void tryToProcessNewJob() {
        if (pendingJobs.size() != 0) {
            Job firstJob = pendingJobs.getFirst();
            if (availableResources.getNumFreeCpus() >= firstJob.getNumCPUs() && availableResources.getFreeMemInMbs() >= firstJob.getMemoryInMbs()) {
                availableResources.allocate(firstJob.getNumCPUs(), firstJob.getMemoryInMbs());
                pendingJobs.removeFirst();
                activeJobs.add(firstJob);
                
                Snapshot.startingJob(self, firstJob.getId(), firstJob.getJobsInBatch(), pendingJobs.size(), availableResources.getNumFreeCpus(), availableResources.getFreeMemInMbs());

                RequestResources.Response res = new RequestResources.Response(self, firstJob.getInitiator(), true, firstJob.getId(), firstJob.getJobsInBatch());
                trigger(res, networkPort);
                tryToProcessNewJob();
            }
        }
    }

    /*
     * When receiving a RequestResources.Request message, its informations are stored in a Job instance
     * Then, this instance is added to the pendingJobs linked list until resources are available
     * When resources are available, this instance is popped from the linked list and added to the activeJobs set
     * Upon reception of the appropriate RequestResources.Release message, it is removed from the activeJobs set 
     */
    class Job {
        /**
         * Weight when trying to guess if a ressource is more CPU or more
         * memory.
         */
        static final int CPU_WEIGHT = 2000;
        /**
         * Weight when trying to guess if a ressource is more CPU or more
         * memory.
         */
        static final int MEMORY_WEIGHT = 1;

        private final Address initiator;
        private final int jobsInBatch;
        private final long id;
        private final int numCPUs;
        private final int amountMemInMb;
        private final ResourceType type;

        public Job(long id, int jobsInBatch, int numCPUs, int amountMemInMb, Address initiator) {
            this.id = id;
            this.jobsInBatch = jobsInBatch;
            this.numCPUs = numCPUs;
            this.amountMemInMb = amountMemInMb;
            this.initiator = initiator;

            // guess if a resource is more CPU or more Memory
            long score = (numCPUs * CPU_WEIGHT - amountMemInMb * MEMORY_WEIGHT);
            if (score > 0) {
                type = ResourceType.CPU;
            } else {
                type = ResourceType.MEMORY;
            }
        }

        public int getJobsInBatch() {
            return jobsInBatch;
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

        public ResourceType getResourceType() {
            return type;
        }

        @Override
        public int hashCode() {
            int hash = 3;
            hash = 59 * hash + (int) (this.id ^ (this.id >>> 32));
            return hash;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            final Job other = (Job) obj;
            return this.id == other.id;
        }
    }

    private class ManagedJob extends Job {

        private final int timeToHoldResource;

        public ManagedJob(long id, int jobsInBatch, int numCPUs, int amountMemInMb, Address initiator, int timeToHoldResource) {
            super(id, jobsInBatch, numCPUs, amountMemInMb, initiator);
            this.timeToHoldResource = timeToHoldResource;
        }

        public int getTimeToHoldResource() {
            return timeToHoldResource;
        }

    }

    /**
     * Pick at most maxProbes among peers+{self}.
     */
    private ArrayList<Address> selectProbes(int maxProbes, ArrayList<Address> peers) {
        ArrayList<Address> randomList = new ArrayList<Address>();
        randomList.add(self);
        randomList.addAll(peers);
        
        int nbProbes = Math.min(maxProbes, randomList.size());
        ArrayList<Address> selectedList;
        Collections.shuffle(randomList, random);
        selectedList = new ArrayList<Address>(randomList.subList(0, nbProbes));
        return selectedList;
    }
}
