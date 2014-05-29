package tman.system.peer.tman;

import common.peer.ResourceType;
import java.util.ArrayList;


import se.sics.kompics.Event;
import se.sics.kompics.address.Address;


public class TManSample extends Event {
	ArrayList<Address> partners = new ArrayList<Address>();
        final private ResourceType type;


	public TManSample(ResourceType type, ArrayList<Address> partners) {
                this.type = type;
		this.partners = new ArrayList<Address>(partners);
	}
        
	public TManSample() {
                type = null;
	}

        public ResourceType getType() {
            return type;
        }
        
	public ArrayList<Address> getSample() {
		return this.partners;
	}
}
