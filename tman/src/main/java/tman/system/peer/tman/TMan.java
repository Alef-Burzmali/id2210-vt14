package tman.system.peer.tman;

import common.configuration.TManConfiguration;
import common.peer.AvailableResources;
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
            trigger(new TManSample(partnerAddresses), tmanPort);
            
            // Active part of TMan
            // Send a partial view to a selected peer.
            UUID requestId = event.getTimeoutId();
            PeerDescriptor peer = selectPeer();
            
            // our peer list is empty, wait until there is more.
            if (peer == null) {
                return;
            }
            
            DescriptorBuffer buffer = prepareBuffer(peer);
            ExchangeMsg.Request request = new ExchangeMsg.Request(requestId, buffer, self, peer.getAddress());
            trigger(request, networkPort);
        }
    };

    Handler<CyclonSample> handleCyclonSample = new Handler<CyclonSample>() {
        @Override
        public void handle(CyclonSample event) {
            List<Address> cyclonPartners = event.getSample();

            // merge cyclonPartners into TManPartners
        }
    };

    /**
     * Merge a received buffer and send our view in response.
     * Passive part of TMan.
     */
    Handler<ExchangeMsg.Request> handleTManPartnersRequest = new Handler<ExchangeMsg.Request>() {
        @Override
        public void handle(ExchangeMsg.Request event) {
            UUID requestId = event.getRequestId();
            Address peer = event.getSource();
            DescriptorBuffer buffer = prepareBuffer(peer);
            ExchangeMsg.Response response = new ExchangeMsg.Response(requestId, buffer, self, peer);
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
            mergeBuffer(event.getSelectedBuffer());
        }
    };
    
    /**
     * Prepare a buffer of nodes to be sent to a given node.
     * m is hardcoded to 10.
     */
    private DescriptorBuffer prepareBuffer(PeerDescriptor peer) {
        descriptorAge++;
        PeerDescriptor selfDescriptor = new PeerDescriptor(self, descriptorAge, availableResources);
        
        ArrayList<PeerDescriptor> buffer = new ArrayList<PeerDescriptor>(tmanPartners);
        buffer.add(selfDescriptor);
        
        //@TODO sort buffer with rank function
        return new DescriptorBuffer(selfDescriptor, buffer.subList(0, 10));
    }
    
    /**
     * Select a peer from our view to send our view.
     * Psi is hardcoded to 5.
     */
    private PeerDescriptor selectPeer() {
        int max = Math.min(tmanPartners.size(), 5);
        
        // list is empty
        if (max <= 0) {
            return null;
        }
        
        int randomIndex = r.nextInt(max);
        return tmanPartners.get(randomIndex);
    }
    
    /**
     * Merge a received buffer in our partial view.
     */
    private void mergeBuffer(DescriptorBuffer buffer) {
        List<PeerDescriptor> peers = buffer.getDescriptors();
        
        ArrayList<PeerDescriptor> set = new ArrayList<PeerDescriptor>(tmanPartners);
        
        for (PeerDescriptor p : peers) {
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
        tmanPartners = set;
    }

    // TODO - if you call this method with a list of entries, it will
    // return a single node, weighted towards the 'best' node (as defined by
    // ComparatorById) with the temperature controlling the weighting.
    // A temperature of '1.0' will be greedy and always return the best node.
    // A temperature of '0.000001' will return a random node.
    // A temperature of '0.0' will throw a divide by zero exception :)
    // Reference:
    // http://webdocs.cs.ualberta.ca/~sutton/book/2/node4.html
    public Address getSoftMaxAddress(List<Address> entries) {
        Collections.sort(entries, new ComparatorById(self));

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
}
