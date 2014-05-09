package resourcemanager.system.peer.rm;

import java.util.List;
import se.sics.kompics.address.Address;
import se.sics.kompics.network.Message;
import se.sics.kompics.timer.ScheduleTimeout;
import se.sics.kompics.timer.Timeout;

/**
 * User: jdowling
 */
public class RequestResources  {

    public static class Request extends Message {

    	/*
    	 * Added the id field to allow us to keep track of particular jobs
    	 */
        private final int numCpus;
        private final int amountMemInMb;
        private final long id;
        private final long idBatch;

        public Request(Address source, Address destination, int numCpus, int amountMemInMb, long id, long idBatch) {
            super(source, destination);
            this.numCpus = numCpus;
            this.amountMemInMb = amountMemInMb;
            this.id = id;
            this.idBatch = idBatch;
        }

        public int getMemoryInMbs() {
            return amountMemInMb;
        }

        public int getNumCpus() {
            return numCpus;
        }
        
        public long getId(){
			return id;
		}
        
        public long getIdBatch() {
			return idBatch;
		}

    }
    
    public static class Response extends Message {
    	
    	/*
    	 * Added the id field to allow us to keep track of particular jobs
    	 */
        private final boolean success;
        private final long id;
        private final long idBatch;
        public Response(Address source, Address destination, boolean success, long id, long idBatch) {
            super(source, destination);
            this.success = success;
            this.id = id;
            this.idBatch = idBatch;
        }
        
        public long getId(){
			return id;
		}
        
        public long getIdBatch() {
			return idBatch;
		}
    }
    
    /*
     * The Release message is sent by the requesting RM to the worker.
     * It is sent after the requesting RM's job has finished (i.e. the job has reached its timeout)
     * It allows the worker to release its resources 
     */
    public static class Release extends Message {
    	private final long id;
    	
    	public Release(Address source, Address destination, long id) {
            super(source, destination);
            this.id = id;
        }

		public long getId(){
			return id;
		}
    }
    
    public static class RequestTimeout extends Timeout {
        private final Address destination;
        RequestTimeout(ScheduleTimeout st, Address destination) {
            super(st);
            this.destination = destination;
        }

        public Address getDestination() {
            return destination;
        }
    }
}
