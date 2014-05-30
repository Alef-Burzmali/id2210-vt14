package resourcemanager.system.peer.rm;

import se.sics.kompics.address.Address;
import se.sics.kompics.timer.ScheduleTimeout;
import se.sics.kompics.timer.Timeout;

public class JobProcessingTimeout extends Timeout {

    private final long id;
    private final Address worker;
    private final int jobsInBatch;

    public JobProcessingTimeout(ScheduleTimeout request, long id, Address worker, int jobsInBatch) {
        super(request);
        this.id = id;
        this.worker = worker;
        this.jobsInBatch = jobsInBatch;
    }

    public long getId() {
        return id;
    }

    public Address getWorker() {
        return worker;
    }
    
    public int getJobsInBatch() {
        return jobsInBatch;
    }
}
