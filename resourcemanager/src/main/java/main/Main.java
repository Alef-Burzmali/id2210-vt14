package main;

import simulator.core.DataCenterSimulationMain;
import common.configuration.Configuration;
import common.simulation.scenarios.Scenario;
import common.simulation.scenarios.Scenario1;

public class Main {

    public static void main(String[] args) throws Throwable {
        // TODO - change the random seed, have the user pass it in.
        long seed = System.currentTimeMillis();
        
        /*
         * Select which samples to use.
         * If true, ResourceManager will use the TMan samples (gradient)
         * If false, ResourceManager will use the Cyclon samples (random)
         */
        boolean useGradientWithTman = true;
        
        // NB: configuration creates files to pass the configuration to other modules
        Configuration configuration = new Configuration(seed, useGradientWithTman);

        Scenario scenario = new Scenario1();
        scenario.setSeed(seed);
        scenario.getScenario().simulate(DataCenterSimulationMain.class);
    }
}
