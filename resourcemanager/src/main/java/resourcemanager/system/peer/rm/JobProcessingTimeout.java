package resourcemanager.system.peer.rm;

import se.sics.kompics.address.Address;
import se.sics.kompics.timer.ScheduleTimeout;
import se.sics.kompics.timer.Timeout;

public class JobProcessingTimeout extends Timeout {
	
	private final long id;
	private final Address worker;
	
	public JobProcessingTimeout(ScheduleTimeout request, long id, Address worker) {
		super(request);
		this.id = id;
		this.worker = worker;
	}

	public long getId() {
		return id;
	}

	public Address getWorker() {
		return worker;
	}
}
