/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package common.simulation.scenarios;

import se.sics.kompics.p2p.experiment.dsl.SimulationScenario;

@SuppressWarnings("serial")
public class Scenario6 extends Scenario {

    private final static SimulationScenario scenario = new SimulationScenario() {
        {

            SimulationScenario.StochasticProcess process0 = new SimulationScenario.StochasticProcess() {
                {
                    eventInterArrivalTime(constant(100));
                    raise(100, Operations.peerJoin(),
                            uniform(0, Integer.MAX_VALUE),
                            constant(8), constant(12000)
                    );
                }
            };

            SimulationScenario.StochasticProcess process1 = new SimulationScenario.StochasticProcess() {
                {
                    eventInterArrivalTime(uniform(1000 * 2, 1000 * 5)); // 2 ms
                    raise(700, Operations.requestResources(),
                            uniform(0, Integer.MAX_VALUE),
                            uniform(1, 6), uniform(1000, 7000),
                            uniform(1000 * 200, 1000 * 500) // 200 ms - 500 ms
                    );
                }
            };
            
            SimulationScenario.StochasticProcess terminateProcess = new SimulationScenario.StochasticProcess() {
                {
                    eventInterArrivalTime(constant(100));
                    raise(1, Operations.terminate);
                }
            };

            process0.start();
            process1.startAfterTerminationOf(2000, process0);
            terminateProcess.startAfterTerminationOf(900 * 1000, process1);
        }
    };

    // -------------------------------------------------------------------
    public Scenario6() {
        super(scenario);
    }
}
