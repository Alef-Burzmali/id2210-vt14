/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package common.simulation.scenarios;

import se.sics.kompics.p2p.experiment.dsl.SimulationScenario;

@SuppressWarnings("serial")
public class Scenario5 extends Scenario {

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
                    eventInterArrivalTime(constant(500)); // 0.5 ms
                    raise(790, Operations.requestResources(),
                            uniform(0, Integer.MAX_VALUE),
                            constant(2), constant(1000),
                            constant(1000 * 200) // 200 ms
                    );
                }
            };
            
            StochasticProcess process2 = new StochasticProcess() {
                {
                    eventInterArrivalTime(constant(500)); // 0.5 ms
                    raise(10, Operations.requestBatchResources(),
                            uniform(0, Integer.MAX_VALUE),
                            constant(10),
                            constant(2), constant(1000),
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
            process2.startAfterTerminationOf(0, process1);
            terminateProcess.startAfterTerminationOf(500 * 1000, process1);
        }
    };

    // -------------------------------------------------------------------
    public Scenario5() {
        super(scenario);
    }
}
