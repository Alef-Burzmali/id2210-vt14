package tman.system.peer.tman;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import se.sics.kompics.address.Address;


public class DescriptorBuffer implements Serializable {
	private static final long serialVersionUID = -4414783055393007206L;
	private final PeerDescriptor from;
	private final ArrayList<PeerDescriptor> descriptors;


	public DescriptorBuffer(PeerDescriptor from,
			List<PeerDescriptor> descriptors) {
		super();
		this.from = from;
		this.descriptors = new ArrayList<PeerDescriptor>(descriptors);
	}


	public PeerDescriptor getFrom() {
		return from;
	}


	public int getSize() {
		return descriptors.size();
	}


	public ArrayList<PeerDescriptor> getDescriptors() {
		return descriptors;
	}
}
