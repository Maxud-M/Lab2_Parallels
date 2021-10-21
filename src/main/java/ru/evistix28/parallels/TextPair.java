package ru.evistix28.parallels;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.file.tfile.RawComparable;
import org.apache.hadoop.mapreduce.Partitioner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TextPair implements WritableComparable<TextPair> {

    static int AeroportId;
    static int dataType;

    @Override
    public int compareTo(TextPair o) {
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

    }


    public class FirstPartitioner<TextPair, Text> extends Partitioner<TextPair, Text> {
        @Override
        public int getPartition(TextPair key, Text value, int numReduceTasks) {
            return (key.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
        }
    }

    public class FirstComparator implements RawComparator<TextPair> {


        public int compare(TextPair a, TextPair b) {
            return a.compareTo(b);
        }

        @Override
        public boolean equals(Object obj) {
            return false;
        }

        @Override
        public int compare(byte[] bytes, int i, int i1, byte[] bytes1, int i2, int i3) {
            return 0;
        }
    }
}
