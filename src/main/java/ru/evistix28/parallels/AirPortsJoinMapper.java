package ru.evistix28.parallels;

import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AirPortsJoinMapper extends Mapper {
    @Override
    protected void map(Object key, Object value, Context context) throws IOException, InterruptedException {
        super.map(key, value, context);
    }
}
