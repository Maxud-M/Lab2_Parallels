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

    private int airportId;
    private int dataType;

    public int getKey() {return airportId;}
    public int getDataType() {return dataType;}




    public TextPair() {}

    public TextPair(int airportId, int dataType) {
        this.airportId = airportId;
        this.dataType = dataType;
    }



    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(airportId);
        dataOutput.writeInt(dataType);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        airportId = dataInput.readInt();
        dataType = dataInput.readInt();

    }

    @Override
    public int compareTo(TextPair o) {
        return airportId - o.airportId;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof TextPair) {
            TextPair tp = (TextPair) o;
            return airportId == tp.airportId && dataType == tp.dataType;
        }
        return false;
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


        public int compare(WritableComparable a, WritableComparable b) {
            a = (TextPair) a;
            b = (TextPair) b;
            return a.compareTo(b);
        }



    }
}
