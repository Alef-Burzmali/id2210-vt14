package resourcemanager.system.peer.rm;

import common.peer.ResourceType;
import se.sics.kompics.address.Address;
import se.sics.kompics.network.Message;

/**
 *
 * @author nicolasgoutay
 *
 */
public class Probe {

    /*
     * Probe.Request message
     * Sent to a fixed number of random RMs to gather informations about ther load
     * The id field used to associate probes with a particular job
     */
    public static class Request extends Message {

        private final long id;
        private final int nbProbes;
        private final ResourceType type;

        public Request(Address source, Address destination, long id, int nbProbes, ResourceType type) {
            super(source, destination);
            this.id = id;
            this.nbProbes = nbProbes;
            this.type = type;
        }

        public int getNbProbes() {
            return nbProbes;
        }

        public long getId() {
            return id;
        }
        
        public ResourceType getType() {
            return type;
        }
    }

    /*
     * Probe.Response message
     * Sent back to the requesting RM to inform it of our load
     * The id field used to associate probes with a particular job
     */
    public static class Response extends Message {

        private final long id;
        private final int nbPendingJobs;
        private final int nbProbes;
        private final ResourceType type;

        public Response(Address source, Address destination, long id, int nbPendingJobs, int nbProbes, ResourceType type) {
            super(source, destination);
            this.id = id;
            this.nbPendingJobs = nbPendingJobs;
            this.nbProbes = nbProbes;
            this.type = type;
        }

        public int getNbProbes() {
            return nbProbes;
        }

        public long getId() {
            return id;
        }

        public int getNbPendingJobs() {
            return nbPendingJobs;
        }
        
        public ResourceType getType() {
            return type;
        }
    }

}
