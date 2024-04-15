package org.example.stats;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MedianStdDevTuple implements Writable {

    private double median;
    private double std;

    public double getMedian() {
        return median;
    }

    public void setMedian(double median) {
        this.median = median;
    }

    public double getStd() {
        return std;
    }

    public void setStd(double std) {
        this.std = std;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(median);
        dataOutput.writeDouble(std);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
          median = dataInput.readDouble();
          std = dataInput.readDouble();
    }
}
