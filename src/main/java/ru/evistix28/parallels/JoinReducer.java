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
        Text airportName = new Text(iter.next());
        float minTimeOfDelay = Integer.MAX_VALUE;
        float maxTimeOfDelay = Integer.MIN_VALUE;
        float sumOfDelay = 0;
        float averageDelay;
        int numOfValues = 0;
        while(iter.hasNext()) {
            Text value = iter.next();
            float flightDelay = Float.parseFloat(value.toString());
            if(minTimeOfDelay < flightDelay)
                minTimeOfDelay = flightDelay;
            if(maxTimeOfDelay > flightDelay)
                maxTimeOfDelay = flightDelay;
             sumOfDelay += flightDelay;
             numOfValues++;
        }
        averageDelay = sumOfDelay / numOfValues;
        Text outValue = new Text(airportName.toString()
                + "\t" + String.valueOf(minTimeOfDelay)
                + "\t" + String.valueOf(maxTimeOfDelay)
                + "\t" + String.valueOf(averageDelay));
        Text outKey = new Text(String.valueOf(key.getKey()));
        context.write(outKey, outValue);

        }
    }

