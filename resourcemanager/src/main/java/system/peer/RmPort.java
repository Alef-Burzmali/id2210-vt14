package system.peer;

import common.simulation.RequestBatchResource;
import common.simulation.RequestResource;
import se.sics.kompics.PortType;

public class RmPort extends PortType {{
	positive(RequestResource.class);
	positive(RequestBatchResource.class);
}}
