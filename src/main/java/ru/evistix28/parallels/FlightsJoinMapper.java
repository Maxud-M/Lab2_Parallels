package ru.evistix28.parallels;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlightsJoinMapper extends Mapper<LongWritable, Text, TextPair, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String val = value.toString();
        String[] data = val.split("\n");
        for(int i = 1; i < data.length; ++i) {
            String[] raw = data[i].split(",");
            context.write(new TextPair(Integer.parseInt(raw[14]), 1), new Text(raw[18]));
        }
    }

}