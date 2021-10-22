package ru.evistix28.parallels;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class JoinReducer extends Reducer<TextPair, Text, Text, Text> {
    @Override
    protected void reduce(TextPair key, Iterable<Text> values, Context context) throws
            IOException, InterruptedException {
        Iterator<Text> iter = values.iterator();
        Text aeroportName = new Text(iter.next());
        int minTimeOfFlight = Integer.MAX_VALUE;
        Text outValue;
        while(iter.hasNext()) {
            Text flight = iter.next();
            if(minTimeOfFlight < Integer.parseInt(flight.toString())) {
                minTimeOfFlight = Integer.parseInt(flight.toString());
            }
        }
        outValue = new Text(aeroportName + "\t" + String.valueOf(minTimeOfFlight));
        context.write(new Text(String.valueOf(key.aeroportId)), outValue);

        }
    }

