package org.bigdata.common;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class PairComparator extends WritableComparator {
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        PairTuple t1 = (PairTuple) a;
        PairTuple t2 = (PairTuple) b;
        return t1.compareTo(t2);
    }
}
