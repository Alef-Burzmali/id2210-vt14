/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package tman.system.peer.tman;

import common.peer.AvailableResources;
import common.peer.ResourceType;
import java.util.Comparator;

/**
 *
 * @author alefburzmali
 */
public class ComparatorByResource implements Comparator<PeerDescriptor> {
    ResourceType type;
    
    public ComparatorByResource(ResourceType type) {
        this.type = type;
    }
    
    @Override
    public int compare(PeerDescriptor o1, PeerDescriptor o2) {
        AvailableResources r1 = o1.getResources();
        AvailableResources r2 = o2.getResources();
        
        switch (type) {
            case CPU:
                return compareByCPU(r1, r2);
            case MEMORY:
                return compareByMemory(r1, r2);
            default:
                return 1;
        }
    }
    
    private int compareByCPU(AvailableResources r1, AvailableResources r2) {
        return Integer.compare(r1.getNumFreeCpus(), r2.getNumFreeCpus());
    }
    
    private int compareByMemory(AvailableResources r1, AvailableResources r2) {
        return Integer.compare(r1.getFreeMemInMbs(), r2.getFreeMemInMbs());
    }
}
