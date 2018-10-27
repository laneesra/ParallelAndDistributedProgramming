package ru.bmstu.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class FlightsRecord implements Writable {
    private IntWritable destAirportId;
    private Text arrDelayNew;

    public FlightsRecord(int destAirportId, String arrDelayNew) {
        this.destAirportId = new IntWritable(destAirportId);
        this.arrDelayNew = new Text(arrDelayNew);
    }

    public void write(DataOutput out) throws IOException {
        this.destAirportId.write(out);
        this.arrDelayNew.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        this.destAirportId.readFields(in);
        this.arrDelayNew.readFields(in);
    }

    public Text getArrDelayNew() {
        return arrDelayNew;
    }
}