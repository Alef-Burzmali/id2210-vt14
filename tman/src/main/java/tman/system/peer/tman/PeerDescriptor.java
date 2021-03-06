package tman.system.peer.tman;

import common.peer.AvailableResources;
import java.io.Serializable;
import se.sics.kompics.address.Address;


public class PeerDescriptor implements Comparable<PeerDescriptor>, Serializable {
	private static final long serialVersionUID = 1906679375438244117L;
	private final Address peerAddress;
	private final AvailableResources resources;
        /**
         * Version of the current descriptor.
         * Lamport clock counter set by the origin node itself to allow
         * comparison of two descriptors for the same node. The more recent
         * one has the more recent resources and is the oe we want to keep.
         */
        private final int age;

        /**
         * Descriptor of a peer for TMan.
         */
	public PeerDescriptor(Address peerAddress, int age, AvailableResources resources) {
		this.peerAddress = peerAddress;
		this.resources = resources;
                this.age = age;
	}

	public AvailableResources getResources() {
		return resources;
	}


	public Address getAddress() {
                return peerAddress;
	}
        
        public int getAge() {
                return age;
        }


	@Override
	public int compareTo(PeerDescriptor that) {
		if (this.age > that.age)
			return 1;
		if (this.age < that.age)
			return -1;
		return 0;
	}


	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((peerAddress == null) ? 0 : peerAddress.hashCode());
		return result;
	}


	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		PeerDescriptor other = (PeerDescriptor) obj;
		if (peerAddress == null) {
			if (other.peerAddress != null)
				return false;
		} else if (!peerAddress.equals(other.peerAddress))
			return false;
		return true;
	}


	@Override
	public String toString() {
		return peerAddress + "";
	}
	
}
