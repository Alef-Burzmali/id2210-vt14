/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package common.simulation.scenarios;

import se.sics.kompics.p2p.experiment.dsl.SimulationScenario;

@SuppressWarnings("serial")
public class Scenario4 extends Scenario {

    private final static SimulationScenario scenario = new SimulationScenario() {
        {

            StochasticProcess process0 = new StochasticProcess() {
                {
                    eventInterArrivalTime(constant(100));
                    raise(100, Operations.peerJoin(),
                            uniform(0, Integer.MAX_VALUE),
                            constant(8), constant(12000)
                    );
                }
            };

            StochasticProcess process1 = new StochasticProcess() {
                {
                    eventInterArrivalTime(constant(1000 * 3)); // 3 ms
                    raise(300, Operations.requestResources(),
                            uniform(0, Integer.MAX_VALUE),
                            constant(6), constant(1000),
                            constant(1000 * 200) // 200 ms
                    );
                }
            };

            StochasticProcess process2 = new StochasticProcess() {
                {
                    eventInterArrivalTime(constant(1000 * 3)); // 3 ms
                    raise(300, Operations.requestResources(),
                            uniform(0, Integer.MAX_VALUE),
                            constant(1), constant(7000),
                            constant(1000 * 200) // 200 ms
                    );
                }
            };

            StochasticProcess process3 = new StochasticProcess() {
                {
                    eventInterArrivalTime(constant(1000 * 3)); // 3 ms
                    raise(100, Operations.requestResources(),
                            uniform(0, Integer.MAX_VALUE),
                            constant(3), constant(6000),
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
            process2.startAfterTerminationOf(2666, process0);
            process3.startAfterTerminationOf(3333, process0);
            terminateProcess.startAfterTerminationOf(900 * 1000, process1);
        }
    };

    // -------------------------------------------------------------------
    public Scenario4() {
        super(scenario);
    }
}
