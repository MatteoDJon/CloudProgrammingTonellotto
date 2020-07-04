package it.unipi.hadoop.sampling;

import org.apache.hadoop.io.Text;

class PriorityLinePair {
    double priority;
    Text line;

    public PriorityLinePair(double priority, Text line) {
        this.priority = priority;
        this.line = line;
    }
}