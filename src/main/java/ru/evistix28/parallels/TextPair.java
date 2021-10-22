package ru.evistix28.parallels;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.file.tfile.RawComparable;
import org.apache.hadoop.mapreduce.Partitioner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TextPair implements WritableComparable<TextPair> {

    int aeroportId;
    int dataType;

    public TextPair() {}

    public TextPair(int aeroportId, int dataType) {
        this.aeroportId = aeroportId;
        this.dataType = dataType;
    }



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


    public static class FirstPartitioner extends Partitioner<TextPair, Text>{

        public FirstPartitioner(){}


        @Override
        public int getPartition(TextPair key, Text value, int numReduceTasks) {
            return (key.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
        }
    }

    public static class FirstComparator extends WritableComparator {

        public FirstComparator(){}


        public int compare(TextPair a, TextPair b) {
            return a.compareTo(b);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof TextPair) {
                TextPair tp = (TextPair) obj;
                return aeroportId == tp.aeroportId && dataType == tp.dataType;
            }
            return false;
        }

    }
}
