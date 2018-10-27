package ru.bmstu.hadoop;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class HashPartitioner extends Partitioner<AirportIdKey, Text> {
    @Override
    public int getPartition(AirportIdKey key, Text value, int numReduceTasks) {
        return (key.getAirportId().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
}
