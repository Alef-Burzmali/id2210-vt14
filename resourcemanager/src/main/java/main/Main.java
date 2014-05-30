package main;

import common.configuration.Configuration;
import common.simulation.scenarios.*;
import simulator.core.DataCenterSimulationMain;

public class Main {

    public static void main(String[] args) throws Throwable {
        // TODO - change the random seed, have the user pass it in.
        long seed = System.currentTimeMillis();
        
        /**
         * Select which samples to use.
         * If true, ResourceManager will use the TMan samples (gradient)
         * If false, ResourceManager will use the Cyclon samples (random)
         */
        boolean useGradientWithTman = true;
        
        // NB: configuration creates files to pass the configuration to other modules
        Configuration configuration = new Configuration(seed, useGradientWithTman);

        /**
         * Select the scenario to test.
         * Scenario 1: a bit less tasks than what can be processed at once
         *      expected result -> no waiting at all
         * Scenario 2: three times more tasks than what can be processed at once
         *      expected result -> lower waiting with gradient
         * Scenario 3: same as Sc2, but all the requests arrive at once
         *      expected result -> the same
         *      what happen -> The gradient is much worse than the random
         *          we think the gradient is not updated rapidly enough
         *          and the overlay forms clusters, which prevent a good
         *          repartition of the load.
         * Scenario 4: 3 different kinds of tasks -> mostly CPU, mostly memory and mixed
         *      expected result -> better performance with the gradient
         * Scenario 6: completly random tasks
         */
        Scenario scenario = new Scenario5();
        scenario.setSeed(seed);
        scenario.getScenario().simulate(DataCenterSimulationMain.class);
    }
}
