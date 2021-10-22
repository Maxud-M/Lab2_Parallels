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
        String airportName = iter.next().toString();
        if(!iter.hasNext()) {
            return;
        }
        float minTimeOfDelay = Float.MAX_VALUE;
        float maxTimeOfDelay = Float.MIN_VALUE;
        float sumOfDelay = Constants.ZERO_FLOAT;
        float averageDelay;
        int numOfValues = 0;
        while(iter.hasNext()) {
            Text value = iter.next();
            float flightDelay = Float.parseFloat(value.toString());
            if(minTimeOfDelay > flightDelay)
                minTimeOfDelay = flightDelay;
            if(maxTimeOfDelay < flightDelay)
                maxTimeOfDelay = flightDelay;
             sumOfDelay += flightDelay;
             numOfValues++;
        }
        averageDelay = sumOfDelay / numOfValues;
        Text outValue = new Text("min:" + "\t" + String.valueOf(minTimeOfDelay) + "\n" +
                                        "max:" + "\t" + String.valueOf(maxTimeOfDelay)  + "\n" +
                                     "average:" + "\t" + String.valueOf(averageDelay) + "\n");
        Text outKey = new Text(airportName);
        context.write(outKey, outValue);

        }
    }

