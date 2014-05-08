package resourcemanager.system.peer.rm;

import common.configuration.RmConfiguration;
import common.peer.AvailableResources;
import common.simulation.RequestResource;
import cyclon.system.peer.cyclon.CyclonSample;
import cyclon.system.peer.cyclon.CyclonSamplePort;
import cyclon.system.peer.cyclon.PeerDescriptor;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import se.sics.kompics.timer.Timer;
import se.sics.kompics.web.Web;
import system.peer.RmPort;
import tman.system.peer.tman.TManSample;
import tman.system.peer.tman.TManSamplePort;

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

	
    public ResourceManager() {

        subscribe(handleInit, control);
        subscribe(handleCyclonSample, cyclonSamplePort);
        subscribe(handleRequestResource, indexPort);
        subscribe(handleUpdateTimeout, timerPort);
        subscribe(handleResourceAllocationRequest, networkPort);
        subscribe(handleResourceAllocationResponse, networkPort);
        subscribe(handleTManSample, tmanPort);
        /*
         * handle probe request
         * handle probe response
         * handle ressource release request (timeout)
         */
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
        	 * If there is enough available resources for the first job of the queue: 
        	 * 		pop the first job of the queue and add it to the activeJobs set
        	 * 		update available resources
        	 * 		send a ResourceAllocationResponse event
        	 */
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
    
    Handler<CyclonSample> handleCyclonSample = new Handler<CyclonSample>() {
        @Override
        public void handle(CyclonSample event) {
            System.out.println("Received samples: " + event.getSample().size());
            
            // receive a new list of neighbours
            neighbours.clear();
            neighbours.addAll(event.getSample());

        }
    };
	
    Handler<RequestResource> handleRequestResource = new Handler<RequestResource>() {
        @Override
        public void handle(RequestResource event) {
            
            System.out.println("Allocate resources: " + event.getNumCpus() + " + " + event.getMemoryInMbs());
            // TODO: 
            /*
             * We pick a fixed number of neighbours at random
             * We send each one a probe (with the id of the job) to check their loads
             */
        }
    };
    
    /*
     * handler probeRequest
     * 		send back a probeResponse containing the id of the job and the number of jobs in the queue
     */
    
    /*
     * handler probeResponse
     * 		add the probe into the hashmap
     * 		if we received a fixed number of probes
     * 			pick the least loaded worker
     * 			retreive the job in the jobs hashtable
     * 			send a resourceAllocationRequest to this worker 
     */
    
    /*
     * handler jobTimeout
     * 		send a resourceRelease to the worker with the id of the job
     */
    
    Handler<TManSample> handleTManSample = new Handler<TManSample>() {
        @Override
        public void handle(TManSample event) {
            // TODO: 
        }
    };

}