package ru.evistix28.parallels;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AirPortsJoinMapper extends Mapper<LongWritable, Text, TextPair, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String val = value.toString();
        String[] data = val.split("\n");
        for(int i = 1; i < data.length; ++i) {
            String[] raw = data[i].split("\t");
            context.write(new TextPair(Integer.parseInt(raw[0]), 0), new Text(raw[1]));
        }
    }
}
