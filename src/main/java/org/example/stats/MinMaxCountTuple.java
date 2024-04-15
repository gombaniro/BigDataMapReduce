package org.example.stats;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;

public class MinMaxCountTuple implements Writable {
    private final static SimpleDateFormat format = new SimpleDateFormat(
            "yyyy-MM-dd'T'HH:mm:ss.SSS"
    );
    private LocalDate min = LocalDate.now();
    private LocalDate max = LocalDate.now();
    private long count;

    public LocalDate getMin() {
        return min;
    }

    public void setMin(LocalDate min) {
        this.min = min;
    }

    public LocalDate getMax() {
        return max;
    }

    public void setMax(LocalDate max) {
        this.max = max;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(min.toEpochDay());
        dataOutput.writeLong(max.toEpochDay());
        dataOutput.writeLong(count);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        // Read the data out in the order it is written,
        // creating new Date objects from the UNIX timestamp
        min = LocalDate.ofEpochDay(dataInput.readLong());
        max = LocalDate.ofEpochDay(dataInput.readLong());
        count = dataInput.readLong();
    }

    public String toString() {
        return format.format(min) + "\t" + format.format(max) + "\t" + count;
    }


}
