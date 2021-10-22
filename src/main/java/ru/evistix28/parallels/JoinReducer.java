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
        int minTimeOfDelay = Integer.MAX_VALUE;
        int maxTimeOfDelay = Integer.MIN_VALUE;
        int sumOfDelay = 0;
        float averageDelay;
        int numOfValues = 0;
        Text outValue;
        while(iter.hasNext()) {
            Text value = iter.next();
            int flightDelay = Integer.parseInt(value.toString());
            if(minTimeOfDelay < flightDelay)
                minTimeOfDelay = flightDelay;
            if(maxTimeOfDelay > flightDelay)
                maxTimeOfDelay = flightDelay;
             sumOfDelay += flightDelay;
             numOfValues++;
        }
        averageDelay = sumOfDelay / numOfValues;
        outValue = new Text(aeroportName
                + "\t" + String.valueOf(minTimeOfDelay)
                + "\t" + String.valueOf(maxTimeOfDelay)
                + "\t" + String.valueOf(averageDelay));
        Text outKey = new Text(String.valueOf(key.g))
        context.write(new Text(String.valueOf(key.aeroportId)), outValue);

        }
    }

