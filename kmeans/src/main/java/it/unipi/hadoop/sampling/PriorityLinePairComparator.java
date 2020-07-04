package it.unipi.hadoop.sampling;

import java.util.Comparator;

class PriorityLinePairComparator implements Comparator<PriorityLinePair> {

    @Override
    public int compare(PriorityLinePair p1, PriorityLinePair p2) {
        if (p1.priority > p2.priority)
            return -1;
        if (p1.priority < p2.priority)
            return 1;
        return 0;
    }
    
}