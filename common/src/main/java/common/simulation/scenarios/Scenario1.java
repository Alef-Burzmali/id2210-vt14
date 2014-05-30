package common.simulation.scenarios;

import se.sics.kompics.p2p.experiment.dsl.SimulationScenario;

@SuppressWarnings("serial")
public class Scenario1 extends Scenario {

    private final static SimulationScenario scenario = new SimulationScenario() {
        {

            StochasticProcess process0 = new StochasticProcess() {
                {
                    eventInterArrivalTime(constant(1000));
                    raise(100, Operations.peerJoin(),
                            uniform(0, Integer.MAX_VALUE),
                            constant(8), constant(12000)
                    );
                }
            };

            StochasticProcess process1 = new StochasticProcess() {
                {
                    eventInterArrivalTime(constant(100)); // 0.1 ms
                    raise(790, Operations.requestResources(),
                            uniform(0, Integer.MAX_VALUE),
                            constant(1), constant(1000),
                            constant(1000 * 200) // 200 ms
                    );
                }
            };

            StochasticProcess terminateProcess = new StochasticProcess() {
                {
                    eventInterArrivalTime(constant(100));
                    raise(1, Operations.terminate);
                }
            };
            process0.start();
            process1.startAfterTerminationOf(2000, process0);
            terminateProcess.startAfterTerminationOf(500 * 1000, process1);
        }
    };

    // -------------------------------------------------------------------
    public Scenario1() {
        super(scenario);
    }
}
