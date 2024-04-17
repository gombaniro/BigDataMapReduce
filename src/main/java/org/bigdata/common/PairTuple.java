package org.bigdata.common;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class PairTuple implements WritableComparable<PairTuple> {
    String value1;
    String value2;

    public PairTuple(String value1, String value2) {
        this.value1 = value1;
        this.value2 = value2;
    }

    public PairTuple() {
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(value1);
        dataOutput.writeUTF(value2);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        value1 = dataInput.readUTF();
        value2 = dataInput.readUTF();
    }

    public String getValue1() {
        return value1;
    }

    public void setValue1(String value1) {
        this.value1 = value1;
    }

    public String getValue2() {
        return value2;
    }

    public void setValue2(String value2) {
        this.value2 = value2;
    }

    @Override
    public String toString() {
        return String.format("%s\t%s\t", value1, value2);
    }

    @Override
    public int compareTo(PairTuple o) {
        int c = this.value1.compareTo(o.value1);
        if (c != 0) {
            return c;
        }
        return this.value2.compareTo(o.value2);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value1, value2);
    }
}
