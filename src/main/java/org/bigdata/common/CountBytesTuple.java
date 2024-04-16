package org.bigdata.common;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class CountBytesTuple implements Writable {

    private long count;
    private long sentBytes;

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public long getSentBytes() {
        return sentBytes;
    }

    public void setSentBytes(long sentBytes) {
        this.sentBytes = sentBytes;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(count);
        dataOutput.writeLong(sentBytes);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        count = dataInput.readLong();
        sentBytes = dataInput.readLong();
    }
}
