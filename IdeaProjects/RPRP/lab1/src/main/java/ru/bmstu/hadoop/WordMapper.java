package ru.bmstu.hadoop;


import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class WordMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String text = value.toString().replaceAll("[\\p{Z}\\p{P}]", " ").toLowerCase(); //"[«\"\\[(),;:.!?\\s]+"
        String[] words = text.split(" +");

        for (String word : words) {
            context.write(new Text(word), new IntWritable(1));
        }
    }
}