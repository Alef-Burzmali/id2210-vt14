package tman.system.peer.tman;

import common.configuration.TManConfiguration;
import common.peer.AvailableResources;
import common.peer.ResourceType;
import se.sics.kompics.Init;
import se.sics.kompics.address.Address;

public final class TManInit extends Init {

    private final Address peerSelf;
    private final TManConfiguration configuration;
    private final AvailableResources availableResources;
    private final ResourceType type;

    public TManInit(Address peerSelf, TManConfiguration configuration,
            AvailableResources availableResources, ResourceType type) {
        super();
        this.peerSelf = peerSelf;
        this.configuration = configuration;
        this.availableResources = availableResources;
        this.type = type;
    }

    public AvailableResources getAvailableResources() {
        return availableResources;
    }

    public Address getSelf() {
        return this.peerSelf;
    }

    public TManConfiguration getConfiguration() {
        return this.configuration;
    }
    
    public ResourceType getType() {
        return type;
    }
}
