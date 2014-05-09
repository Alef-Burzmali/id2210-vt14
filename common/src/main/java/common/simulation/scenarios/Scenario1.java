package common.simulation.scenarios;

import se.sics.kompics.p2p.experiment.dsl.SimulationScenario;

@SuppressWarnings("serial")
public class Scenario1 extends Scenario {
	private static SimulationScenario scenario = new SimulationScenario() {{
                
		StochasticProcess process0 = new StochasticProcess() {{
			eventInterArrivalTime(constant(10));
			raise(100, Operations.peerJoin(), 
                                uniform(0, Integer.MAX_VALUE), 
                                constant(8), constant(12000)
                             );
		}};
                
		StochasticProcess process1h = new StochasticProcess() {{
			eventInterArrivalTime(uniform(100, 1000));
			raise(5000, Operations.requestResources(), 
                                uniform(0, Integer.MAX_VALUE),
                                uniform(1,2), uniform(1000,4000),
                                constant(1000*5000) 
                                );
		}};
		
		StochasticProcess process1l = new StochasticProcess() {{
			System.out.println("\n");
			eventInterArrivalTime(uniform(100, 1000));
			raise(100, Operations.requestResources(), 
                                uniform(0, Integer.MAX_VALUE),
                                uniform(1,2), uniform(1000,4000),
                                constant(1000*5000)
                                );
		}};
		
		StochasticProcess process2 = new StochasticProcess() {{
			eventInterArrivalTime(constant(100));
			raise(10000, Operations.requestBatchResources(), 
                                uniform(0, Integer.MAX_VALUE),
                                constant(2),
                                constant(2), constant(2000),
                                uniform(1000*10, 1000*10*1) // 1 minute
                                );
		}};
                
                // TODO - not used yet
		StochasticProcess failPeersProcess = new StochasticProcess() {{
			eventInterArrivalTime(constant(100));
			raise(1, Operations.peerFail, 
                                uniform(0, Integer.MAX_VALUE));
		}};
                
		StochasticProcess terminateProcess = new StochasticProcess() {{
			eventInterArrivalTime(constant(100));
			raise(1, Operations.terminate);
		}};
		process0.start();
		process1h.startAfterTerminationOf(5000, process0);
//		process1l.startAfterTerminationOf(1000*1000*10, process1h);
//		process2.startAfterTerminationOf(50000, process0);
        terminateProcess.startAfterTerminationOf(1000*1000*100, process1l);
	}};

	// -------------------------------------------------------------------
	public Scenario1() {
		super(scenario);
	}
}
