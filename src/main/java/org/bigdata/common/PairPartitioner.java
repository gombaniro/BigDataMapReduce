package org.bigdata.common;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Partitioner;

public class PairPartitioner<K extends PairTuple, V extends Writable> extends Partitioner<K, V> {

    @Override
    public int getPartition(K key, V value, int numPartitions) {
        // Your custom logic to determine the partition number based on the key
        // Here, we use a simple hash-based partitioning strategy

        // Ensure that numPartitions is greater than 0
        if (numPartitions <= 0) {
            throw new IllegalArgumentException("Number of partitions must be greater than 0");
        }

        // Convert the key to a string and calculate its hash code
        int hashCode = key.getValue1().hashCode();

        // Use modulo operator to map the hash code to a partition number
        int partition = Math.abs(hashCode) % numPartitions;

        return partition;
    }
}

