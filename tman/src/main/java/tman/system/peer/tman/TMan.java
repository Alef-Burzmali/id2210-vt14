package tman.system.peer.tman;

import common.configuration.TManConfiguration;
import common.peer.AvailableResources;
import common.peer.ResourceType;
import java.util.ArrayList;

import cyclon.system.peer.cyclon.CyclonSample;
import cyclon.system.peer.cyclon.CyclonSamplePort;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.UUID;
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
import se.sics.kompics.timer.Timeout;
import se.sics.kompics.timer.Timer;

import tman.simulator.snapshot.Snapshot;

public final class TMan extends ComponentDefinition {

    private static final Logger logger = LoggerFactory.getLogger(TMan.class);

    Negative<TManSamplePort> tmanPort = negative(TManSamplePort.class);
    Positive<CyclonSamplePort> cyclonSamplePort = positive(CyclonSamplePort.class);
    Positive<Network> networkPort = positive(Network.class);
    Positive<Timer> timerPort = positive(Timer.class);
    private long period;
    private Address self;
    private ArrayList<PeerDescriptor> tmanPartners;
    private TManConfiguration tmanConfiguration;
    private Random r;
    private AvailableResources availableResources;
    private ResourceType type;
    
    /**
     * Lamport clock for our own descriptor.
     */
    private int descriptorAge = 0;

    public class TManSchedule extends Timeout {

        public TManSchedule(SchedulePeriodicTimeout request) {
            super(request);
        }

        public TManSchedule(ScheduleTimeout request) {
            super(request);
        }
    }

    public TMan() {
        tmanPartners = new ArrayList<PeerDescriptor>();

        subscribe(handleInit, control);
        subscribe(handleRound, timerPort);
        subscribe(handleCyclonSample, cyclonSamplePort);
        subscribe(handleTManPartnersResponse, networkPort);
        subscribe(handleTManPartnersRequest, networkPort);
    }

    Handler<TManInit> handleInit = new Handler<TManInit>() {
        @Override
        public void handle(TManInit init) {
            self = init.getSelf();
            tmanConfiguration = init.getConfiguration();
            period = tmanConfiguration.getPeriod();
            r = new Random(tmanConfiguration.getSeed());
            availableResources = init.getAvailableResources();
            type = init.getType();
            
            SchedulePeriodicTimeout rst = new SchedulePeriodicTimeout(period, period);
            rst.setTimeoutEvent(new TManSchedule(rst));
            trigger(rst, timerPort);
        }
    };

    /**
     * Initiate a new round of TMan.
     * Sends our sample to the application, and initiate the active
     * part of TMan.
     */
    Handler<TManSchedule> handleRound = new Handler<TManSchedule>() {
        @Override
        public void handle(TManSchedule event) {
            ArrayList<Address> partnerAddresses = new ArrayList<Address>();
            for (PeerDescriptor descriptor : tmanPartners) {
                partnerAddresses.add(descriptor.getAddress());
            } 
            
            Snapshot.updateTManPartners(self, partnerAddresses);

            // Publish sample to connected components
            trigger(new TManSample(type, partnerAddresses), tmanPort);
            
            // Active part of TMan
            // Send a partial view to a selected peer.
            UUID requestId = event.getTimeoutId();
            PeerDescriptor peer = selectPeer();
            
            // our peer list is empty, wait until there is more.
            if (peer == null) {
                return;
            }
            
            DescriptorBuffer buffer = prepareBuffer();
            ExchangeMsg.Request request = new ExchangeMsg.Request(type, requestId, buffer, self, peer.getAddress());
            trigger(request, networkPort);
        }
    };

    /**
     * Add a sample from Cyclon into our view.
     * Cyclon is not aware of resources and uses a different descriptor as us.
     * We cannot add its peers directly, we will need to start a TMan peer-exchange
     * with them to get their Descriptor.
     */
    Handler<CyclonSample> handleCyclonSample = new Handler<CyclonSample>() {
        @Override
        public void handle(CyclonSample event) {
            List<Address> cyclonPartners = event.getSample();
            
            // merge cyclonPartners into TManPartners
            UUID requestId = UUID.randomUUID();
            DescriptorBuffer buffer = prepareBuffer();
            for (Address peer : cyclonPartners) {
                ExchangeMsg.Request request = new ExchangeMsg.Request(type, requestId, buffer, self, peer);
                trigger(request, networkPort);
            }
        }
    };

    /**
     * Merge a received buffer and send our view in response.
     * Passive part of TMan.
     */
    Handler<ExchangeMsg.Request> handleTManPartnersRequest = new Handler<ExchangeMsg.Request>() {
        @Override
        public void handle(ExchangeMsg.Request event) {
            // if the request is not for our type of resource, silently ignore it.
            if (event.getType() != type) {
                return;
            }
            
            UUID requestId = event.getRequestId();
            Address peer = event.getSource();
            DescriptorBuffer buffer = prepareBuffer();
            ExchangeMsg.Response response = new ExchangeMsg.Response(type, requestId, buffer, self, peer);
            trigger(response, networkPort);
            
            mergeBuffer(event.getRandomBuffer());
        }
    };

    /**
     * Merge the buffer received in reponse.
     * Second half of the active part.
     */
    Handler<ExchangeMsg.Response> handleTManPartnersResponse = new Handler<ExchangeMsg.Response>() {
        @Override
        public void handle(ExchangeMsg.Response event) {
            // if the request is not for our type of resource, silently ignore it.
            if (event.getType() != type) {
                return;
            }
             
            mergeBuffer(event.getSelectedBuffer());
        }
    };
    
    /**
     * Prepare a buffer of nodes to be sent to a given node.
     */
    private DescriptorBuffer prepareBuffer() {
        PeerDescriptor selfDescriptor = getSelfDescriptor();
        
        ArrayList<PeerDescriptor> buffer = new ArrayList<PeerDescriptor>(tmanPartners);
        buffer.add(selfDescriptor);
        
        return new DescriptorBuffer(selfDescriptor, buffer);
    }
    
    /**
     * Select a peer from our view to send our view.
     * Psi is hardcoded to 5.
     */
    private PeerDescriptor selectPeer() {
        if (tmanPartners.isEmpty()) {
            return null;
        } else {
            return getSoftMaxAddress(tmanPartners);
        }
    }
    
    /**
     * Merge a received buffer in our partial view.
     * @TODO make the number of nodes kept configurable
     */
    private void mergeBuffer(DescriptorBuffer buffer) {
        List<PeerDescriptor> peers = buffer.getDescriptors();
        
        ArrayList<PeerDescriptor> set = new ArrayList<PeerDescriptor>(tmanPartners);
        
        for (PeerDescriptor p : peers) {
            if (p.getAddress().equals(self)) {
                continue;
            }
            
            int index = set.indexOf(p);
            if (index != -1) {
                PeerDescriptor q = set.get(index);
                
                // if p is newer than q, then its description of AvailableResources is
                // newer too, so we want to keep it.
                if (p.compareTo(q) == 1) {
                    set.set(index, p);
                }
            } else {
                set.add(p);
            }
        }
        
        rank(set);
        int keepNPeers = Math.min(10, set.size());
        tmanPartners = new ArrayList<PeerDescriptor>(set.subList(0, keepNPeers));
    }
    
    /**
     * Inplace ranking function.
     * @param base      Peer at the "center" of our ranking.
     * @param peers     Peers to rank.
     */
    private void rank(List<PeerDescriptor> peers) {
        PeerDescriptor selfDescriptor = new PeerDescriptor(self, descriptorAge, availableResources);
        Collections.sort(peers, new ComparatorByResource(type, selfDescriptor));
    }

    /**
     * Select a random node wieghted toward the first (best) ones.
     * the temperature controlling the weighting.
     * A temperature of '1.0' will be greedy and always return the best node.
     * A temperature of '0.000001' will return a random node.
     * A temperature of '0.0' will throw a divide by zero exception :)
     * Reference:
     * @see http://webdocs.cs.ualberta.ca/~sutton/book/2/node4.html
     * @param entries Sorted list of nodes
     * @return Selected node
     */
    public PeerDescriptor getSoftMaxAddress(List<PeerDescriptor> entries) {
        double rnd = r.nextDouble();
        double total = 0.0d;
        double[] values = new double[entries.size()];
        int j = entries.size() + 1;
        for (int i = 0; i < entries.size(); i++) {
            // get inverse of values - lowest have highest value.
            double val = j;
            j--;
            values[i] = Math.exp(val / tmanConfiguration.getTemperature());
            total += values[i];
        }

        for (int i = 0; i < values.length; i++) {
            if (i != 0) {
                values[i] += values[i - 1];
            }
            // normalise the probability for this entry
            double normalisedUtility = values[i] / total;
            if (normalisedUtility >= rnd) {
                return entries.get(i);
            }
        }
        return entries.get(entries.size() - 1);
    }
    
    /**
     * Construct a PeerDescriptor of ourself
     * @return descriptor of ourself
     */
    private PeerDescriptor getSelfDescriptor() {
        descriptorAge++;
        return new PeerDescriptor(self, descriptorAge, availableResources);
    }
}
