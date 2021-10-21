package ru.evistix28.parallels;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.file.tfile.RawComparable;
import org.apache.hadoop.mapreduce.Partitioner;

public class TextPair {

    static int AeroportId;
    static int dataType;


    public class FirstPartitioner<TextPair, Text> extends Partitioner<TextPair, Text> {
        @Override
        public int getPartition(TextPair key, Text value, int numReduceTasks) {
            return (key.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
        }
    }

    public class FirstComparator implements RawComparator {


        public int compare(TextPairComparable a, TextPairComparable b) {
            return a.compareTo(b);
        }

        @Override
        public boolean equals(Object obj) {
            return false;
        }

    }
}
