package org.bigdata.common;

import org.apache.hadoop.io.Writable;

import java.io.*;

public class PairTuple implements Writable, Comparable<PairTuple>  {
    String value1;
    String value2;

    public PairTuple(String value1, String value2) {
        this.value1 = value1;
        this.value2 = value2;
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
    public String getValue2() {
        return value2;
    }

    @Override
    public int compareTo(PairTuple o) {
        int c = this.value1.compareTo(o.value1);
        if (c != 0) {
            return c;
        }
        return this.value2.compareTo(o.value2);
    }

//    public static void main(String[] args) throws IOException {
//        // Writing PairTuple to DataOutput
//        ByteArrayOutputStream baos = new ByteArrayOutputStream();
//        DataOutput dataOutput = new DataOutputStream(baos);
//        PairTuple tupleToWrite = new PairTuple("hello", "world");
//        tupleToWrite.write(dataOutput);
//
//        // Reading PairTuple from DataInput
//        byte[] bytes = baos.toByteArray();
//        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
//        DataInput dataInput = new DataInputStream(bais);
//        PairTuple tupleToRead = new PairTuple("", "");
//        tupleToRead.readFields(dataInput);
//
//        // Displaying the values read from PairTuple
//        System.out.println("Value 1: " + tupleToRead.getValue1());
//       System.out.println("Value 2: " + tupleToRead.getValue2());
//    }


}
