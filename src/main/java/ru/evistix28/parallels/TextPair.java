package ru.evistix28.parallels;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.file.tfile.RawComparable;
import org.apache.hadoop.mapreduce.Partitioner;

public class TextPair {

    static int AeroportId;
    static int typeData;


    public class FirstPartitioner extends Partitioner {
        @Override
        public int getPartition(Object o, Object o2, int i) {
            return 0;
        }
    }
    public class FirstComparator implements RawComparator {
        @Override
        public int compare(Object o1, Object o2) {
            return 0;
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
