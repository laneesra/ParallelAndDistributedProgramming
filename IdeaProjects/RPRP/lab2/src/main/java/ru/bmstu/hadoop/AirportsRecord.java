package ru.bmstu.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class AirportsRecord implements Writable {
    private IntWritable code;
    private Text description;

    public AirportsRecord(int code, String description) {
        this.code = new IntWritable(code);
        this.description = new Text(description);
    }

    public void write(DataOutput out) throws IOException {
        this.code.write(out);
        this.description.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        this.code.readFields(in);
        this.description.readFields(in);
    }

    public Text getDescription() {
        return description;
    }
}