package ru.bmstu.hadoop;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import ru.bmstu.hadoop.AirportIdKey;

import java.io.IOException;

import static java.lang.Double.max;
import static java.lang.Double.min;

public class JoinReducer extends Reducer<AirportIdKey, Text, Text, Text> {
    @Override
    protected void reduce(AirportIdKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Text airportName = new Text();
        double min = 0, max = 0, mean = 0, count = 0;

        for (Text val : values) {
            if (count == 0) {
                airportName.set(val);
            } else {
                double arrDelayTime = Double.parseDouble(val.toString());
                if (count == 1) {
                    min = arrDelayTime;
                } else {
                    min = min(min, arrDelayTime);
                }
                max = max(max, arrDelayTime);
                mean += arrDelayTime;
            }
            count++;
        }

        if (count != 1) {
            mean /= count - 1;
            context.write(new Text(airportName + ": "), new Text("min = " + min + ", max = " + max + ", mean = " + String.format("%.2f", mean)));
        }
    }
}
