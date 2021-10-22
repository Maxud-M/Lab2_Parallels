package ru.evistix28.parallels;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlightsJoinMapper extends Mapper<LongWritable, Text, TextPair, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if(key.get() != Constants.FIRST_STR_IN_DATA) {
            FlightReader flightReader = new FlightReader(value.toString());
            TextPair keyOut = new TextPair(flightReader.getKey(),
            Text valueOut = new Text(raw[18].replaceAll("\"", ""));
            context.write(keyOut, valueOut);
            }
        }
    }

}