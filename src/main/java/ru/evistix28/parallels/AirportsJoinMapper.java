package ru.evistix28.parallels;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AirportsJoinMapper extends Mapper<LongWritable, Text, TextPair, Text> {


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if(key == )
        AirportReader airportReader = new AirportReader(value.toString());
        TextPair keyOut =
    }
}
