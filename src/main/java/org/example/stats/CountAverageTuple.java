package org.example.stats;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CountAverageTuple implements Writable {

    private long count;
    private double average;

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public double getAverage() {
        return average;
    }

    public void setAverage(double average) {
        this.average = average;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(count);
        dataOutput.writeDouble(average);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        count = dataInput.readLong();
        average = dataInput.readDouble();

    }
}
