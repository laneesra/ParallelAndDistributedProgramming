package ru.bmstu.hadoop;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class


GroupingComparatorClass extends WritableComparator {
    public GroupingComparatorClass() {
        super (AirportIdKey.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        AirportIdKey keyA = (AirportIdKey)a;
        AirportIdKey keyB = (AirportIdKey)b;

        return keyA.getAirportId().compareTo(keyB.getAirportId());
    }
}
