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
            if(flightReader.getFlightDelay() > Constants.ZERO_FLOAT) {
                TextPair keyOut = new TextPair(flightReader.getKey(), Constants.FLIGHT_DATA_TYPE);
                Text valueOut = new Text(String.valueOf(flightReader.getFlightDelay()));
                context.write(keyOut, valueOut);
            }
        }
    }
}