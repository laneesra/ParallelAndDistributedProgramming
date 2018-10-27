package ru.bmstu.hadoop;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import ru.bmstu.hadoop.AirportIdKey;

import java.io.IOException;

public class AirportsMapper extends Mapper<LongWritable, Text, AirportIdKey, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] recordFields = value.toString().split(",", 2);

        if (!key.equals(new IntWritable(0))) {
            int code = Integer.parseInt(recordFields[0].replace("\"", ""));
            String description = recordFields[1].replace("\"", "");;

            AirportIdKey recordKey = new AirportIdKey(code, AirportIdKey.AIRPORTS_RECORD);
            AirportsRecord record = new AirportsRecord(code, description);

            context.write(recordKey, record.getDescription());
        }
    }
}
