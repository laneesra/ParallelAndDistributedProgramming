package ru.bmstu.hadoop;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlightsMapper extends Mapper<LongWritable, Text, AirportIdKey, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] recordFields = value.toString().split(",");

        if (!key.equals(new IntWritable(0))) {
            int destAirportId = Integer.parseInt(recordFields[14]);
            String arrDelayNew = recordFields[18];

            if (!arrDelayNew.equals("") && Double.parseDouble(arrDelayNew) > 0) {
                AirportIdKey recordKey = new AirportIdKey(destAirportId, AirportIdKey.FLIGHTS_RECORD);
                FlightsRecord record = new FlightsRecord(destAirportId, arrDelayNew);

                context.write(recordKey, record.getArrDelayNew());
            }
        }
    }
}
