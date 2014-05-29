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
    private final ResourceType type;
    private final AvailableResources base;
    
    public ComparatorByResource(ResourceType type, PeerDescriptor basePeer) {
        this.base = basePeer.getResources();
        this.type = type;
    }
    
    @Override
    public int compare(PeerDescriptor o1, PeerDescriptor o2) {
        AvailableResources r1 = o1.getResources();
        AvailableResources r2 = o2.getResources();
        
        switch (type) {
            case CPU:
                return compareWithBase(base.getNumFreeCpus(), r1.getNumFreeCpus(), r2.getNumFreeCpus());
            case MEMORY:
                return compareWithBase(base.getFreeMemInMbs(), r1.getFreeMemInMbs(), r2.getFreeMemInMbs());
            default:
                return 1;
        }
    }
    
    /**
     * Order with a base as reference.
     * a is prefered over b if a is greater than the base and b is lower, else
     * if a is nearer to the base than b.
     * @param base
     * @param a
     * @param b
     * @return 
     */
    private int compareWithBase(int base, int a, int b) {
        if (a == b) {
            return 0;
        } else if (a < base && b > base) {
            return 1;
        } else if (b < base && a > base) {
            return -1;
        } else if (Math.abs(a - base) < Math.abs(b - base)) {
            return -1;
        } else {
            return 1;
        }
    }
}
