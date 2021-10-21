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

    int aeroportId;
    int dataType;



    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(aeroportId);
        dataOutput.writeInt(dataType);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        aeroportId = dataInput.readInt();
        dataType = dataInput.readInt();

    }

    @Override
    public int compareTo(TextPair o) {
        return aeroportId - o.aeroportId;
    }


    public class FirstPartitioner extends Partitioner<TextPair, Text>{
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
