package resourcemanager.system.peer.rm;

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
	public static class Request extends Message{
		private final long id;
		
		public Request(Address source, Address destination, long id){
			super(source, destination);
			this.id = id;
		}
		
		public long getId(){
			return id;
		}
	}
	
	/*
	 * Probe.Response message
	 * Sent back to the requesting RM to inform it of our load
	 * The id field used to associate probes with a particular job
	 */
	public static class Response extends Message{
		private final long id;
		private final int nbPendingJobs;
		
		public Response(Address source, Address destination, long id, int nbPendingJobs){
			super(source, destination);
			this.id = id;
			this.nbPendingJobs = nbPendingJobs;
		}
		
		public long getId(){
			return id;
		}
		
		public int getNbPendingJobs(){
			return nbPendingJobs;
		}
	}
	
}
