package resourcemanager.system.peer.rm;

import se.sics.kompics.address.Address;
import se.sics.kompics.network.Message;

/**
 *
 * @author nicolasgoutay
 *
 */
class Probe {

    /*
     * Probe.Request message
     * Sent to a fixed number of random RMs to gather informations about ther load
     * The id field used to associate probes with a particular job
     */
    static class Request extends Message {

        private final long id;
        private final int nbHops;
        private final ResourceManager.Job job;
        private final int nbProbes;

        public Request(Address source, Address destination, long id, int nbProbes, int nbHops, ResourceManager.Job job) {
            super(source, destination);
            this.id = id;
            this.nbProbes = nbProbes;
            this.nbHops = nbHops;
            this.job = job;
        }
        public Request(Request oldReq, Address destination) {
            super(oldReq.getSource(), destination);
            this.id = oldReq.getId();
            this.nbProbes = oldReq.getNbProbes();
            this.nbHops = oldReq.getNbHops() - 1;
            this.job = oldReq.getJob();
        }

        public int getNbHops() {
            return nbHops;
        }
        
        public int getNbProbes() {
            return nbProbes;
        }

        public long getId() {
            return id;
        }
        
        public ResourceManager.Job getJob() {
            return job;
        }
    }
    
    static class Response extends Message {
        private final long id;
        private final ResourceManager.Job job;
        public Response(Address source, Address destination, long id, ResourceManager.Job job) {
            super(source, destination);
            this.id = id;
            this.job = job;
        }
        public Response(Request request) {
            super(request.getDestination(), request.getSource());
            this.id = request.getId();
            this.job = request.getJob();
        }
        public long getId() {
            return id;
        }
        public ResourceManager.Job getJob() {
            return job;
        }
    }
    
    static class Cancel extends Message {
        private final long id;
        private final ResourceManager.Job job;
        public Cancel(Address source, Address destination, long id, ResourceManager.Job job) {
            super(source, destination);
            this.id = id;
            this.job = job;
        }
        public Cancel(Response response) {
            super(response.getDestination(), response.getSource());
            this.id = response.getId();
            this.job = response.getJob();
        }
        public long getId() {
            return id;
        }
        public ResourceManager.Job getJob() {
            return job;
        }
    }
}
