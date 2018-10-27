package ru.bmstu.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AirportIdKey implements WritableComparable<AirportIdKey> {
    private IntWritable airportId = new IntWritable();
    private IntWritable datasetIndicator = new IntWritable(); // 0 - airports; 1 - completed flights;

    public static final IntWritable AIRPORTS_RECORD = new IntWritable(0);
    public static final IntWritable FLIGHTS_RECORD = new IntWritable(1);

    public AirportIdKey() {}

    public AirportIdKey(int airportId, IntWritable datasetIndicator) {
        this.airportId.set(airportId);
        this.datasetIndicator = datasetIndicator;
    }

    public void write(DataOutput out) throws IOException {
        this.airportId.write(out);
        this.datasetIndicator.write(out);
    }

    public void readFields(DataInput in) throws IOException{
        this.airportId.readFields(in);
        this.datasetIndicator.readFields(in);
    }

    public int compareTo(AirportIdKey other) {
        if (this.airportId.equals(other.airportId)) {
            return this.datasetIndicator.compareTo(other.datasetIndicator);
        } else {
            return this.airportId.compareTo(other.airportId);
        }
    }

    public IntWritable getAirportId() {
        return airportId;
    }
}
