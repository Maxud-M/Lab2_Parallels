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

    Text aeroportId;
    Text dataType;

    public TextPair(Text aeroportId, Text dataType) {
        this.aeroportId = aeroportId;
        this.dataType = dataType;
    }



    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(Integer.parseInt(aeroportId.toString()));
        dataOutput.writeInt(Integer.parseInt(dataType.toString()));

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        aeroportId = new Text(String.valueOf(dataInput.readInt()));
        dataType = new Text(String.valueOf(dataInput.readInt()));

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

    public class FirstComparator extends WritableComparator {


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
