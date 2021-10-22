package ru.evistix28.parallels;



import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Partitioner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TextPair implements WritableComparable<TextPair> {

    private int airportId;
    private int dataType;

    public int getKey() {return airportId;}




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
        if(airportId == o.airportId)
            return dataType - o.dataType;
        return airportId - o.airportId;
    }


    public static class FirstPartitioner extends Partitioner<TextPair, Text>{

        public FirstPartitioner(){}


        @Override
        public int getPartition(TextPair key, Text value, int numReduceTasks) {
            return (key.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
        }
    }

    public static class FirstComparator extends WritableComparator {


        public FirstComparator(){super(TextPair.class, true);}

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            return ((TextPair) a).airportId - ((TextPair) b).airportId;
        }
    }
}
