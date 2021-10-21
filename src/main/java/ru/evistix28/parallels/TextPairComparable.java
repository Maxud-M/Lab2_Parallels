package ru.evistix28.parallels;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TextPairComparable implements WritableComparable<TextPair> {

    

    @Override
    public void write(DataOutput dataOutput) throws IOException {

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

    }

    @Override
    public int compareTo(TextPair o) {
        return 0;
    }
}
