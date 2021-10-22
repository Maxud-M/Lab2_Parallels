package ru.evistix28.parallels;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AirportsJoinMapper extends Mapper<LongWritable, Text, TextPair, Text> {

    public static final int FIRST_STR_IN_DATA = 0;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if(key.get() != FIRST_STR_IN_DATA) {
            AirportReader airportReader = new AirportReader(value.toString());
            TextPair keyOut = new TextPair(airportReader.getKey(), 0);
            Text valueOut = new Text(airportReader.getAirportName());
            context.write(keyOut, valueOut);
        }
    }
}
