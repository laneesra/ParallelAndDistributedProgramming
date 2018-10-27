package ru.bmstu.spark;

import scala.Tuple2;

import java.io.Serializable;
import java.util.Map;

public class AirportData implements Serializable {
    private double maxArrDelay;
    private long lateAndCanceled;
    private long total;
    private String originAirportDescription;
    private String destAirportDescription;


    public AirportData(double maxArrDelay, long lateAndCanceled, long total) {
        this.maxArrDelay = maxArrDelay;
        this.lateAndCanceled = lateAndCanceled;
        this.total = total;
    }

    public AirportData(Tuple2<Tuple2<String, String>, AirportData> original, Map<String, String> stringAirportDataMap) {
        this.maxArrDelay = original._2.getMaxArrDelay();
        this.lateAndCanceled = original._2.getLateAndCanceled();
        this.total = original._2.getTotal();
        this.originAirportDescription = stringAirportDataMap.get(original._1._1);
        this.destAirportDescription = stringAirportDataMap.get(original._1._2);
    }

    public double getMaxArrDelay() {
        return maxArrDelay;
    }

    public long getTotal() {
        return total;
    }

    public double getLateAndCanceledPercentage() {
        return (double)lateAndCanceled / total * 100;
    }

    public long getLateAndCanceled() {
        return lateAndCanceled;
    }

    public static AirportData addValue(AirportData a, double arrDelay, long lateAndCanceled) {
        return new AirportData(Double.max(a.maxArrDelay, arrDelay), a.lateAndCanceled + lateAndCanceled, a.total + 1l);
    }

    public static AirportData add(AirportData a, AirportData b) {
        return new AirportData(Double.max(a.maxArrDelay, b.maxArrDelay), a.lateAndCanceled + b.lateAndCanceled, a.total + b.total);
    }

    public String toString() {
        return "flight: " + originAirportDescription + " - " + destAirportDescription +
                "\nmaxArrDelay: " + maxArrDelay +
                "\nlateAndCanceled: " + String.format("%.2f", getLateAndCanceledPercentage()) +
                "\n";
    }

}
