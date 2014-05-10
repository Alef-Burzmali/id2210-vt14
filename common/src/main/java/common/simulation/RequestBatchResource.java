package common.simulation;

import se.sics.kompics.Event;

public final class RequestBatchResource extends Event {
	
	private final long id;
	private final int nbNodes;
    private final int numCpus;
    private final int memoryInMbs;
    private final int timeToHoldResource;

    public RequestBatchResource(long id, int nbNodes, int numCpus, int memoryInMbs, int timeToHoldResource) {
        this.id = id;
        this.nbNodes = nbNodes;
        this.numCpus = numCpus;
        this.memoryInMbs = memoryInMbs;
        this.timeToHoldResource = timeToHoldResource;
    }

    public long getId() {
        return id;
    }

    public int getNbNodes() {
		return nbNodes;
	}

	public int getTimeToHoldResource() {
        return timeToHoldResource;
    }

    public int getMemoryInMbs() {
        return memoryInMbs;
    }

    public int getNumCpus() {
        return numCpus;
    }

}
