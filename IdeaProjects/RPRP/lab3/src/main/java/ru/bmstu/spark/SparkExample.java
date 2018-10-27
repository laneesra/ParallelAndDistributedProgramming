package ru.bmstu.spark;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Map;

public final class SparkExample {
    private static final int ORIGIN_AIRPORT_ID = 11;
    private static final int DEST_AIRPORT_ID = 14;
    private static final int ARR_DELAY_NEW = 18;
    private static final int CANCELLED = 19;
    private static final int CODE = 0;
    private static final int DESCRIPTION = 1;

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: ru.bmstu.hadoop.SparkExample <input path> <input path> <output path>");
            System.exit(-1);
        }

        SparkConf conf = new SparkConf().setAppName("lab3");
        JavaSparkContext sc = new JavaSparkContext(conf);


        JavaRDD<String> flightFile = sc.textFile(args[1]).flatMap(
                s -> Arrays.stream(s.split("\n")).iterator()
        );
        String flightHeader = flightFile.first();


        JavaRDD<FlightRecord> flightRecord = flightFile
                .filter(s -> !s.equals(flightHeader))
                .map(s -> {
                            String[] fields = s.replace("\"", "").split(",");
                            return new FlightRecord(fields[ORIGIN_AIRPORT_ID], fields[DEST_AIRPORT_ID], fields[ARR_DELAY_NEW], fields[CANCELLED]);
                        }
                );

        JavaPairRDD<Tuple2<String, String>, FlightRecord> airportPairs = flightRecord
                .mapToPair(fr -> new Tuple2<>(new Tuple2<>(fr.getOriginAirportId(), fr.getDestAirportId()), fr));

        JavaPairRDD<Tuple2<String, String>, AirportData> airportData = airportPairs
                .combineByKey(
                        ap -> new AirportData(ap.getArrDelay(), ap.isLateOrCanceled(), 1l),
                        (a, b) -> AirportData.addValue(a, b.getArrDelay(), b.isLateOrCanceled()),
                        AirportData::add
                );

        JavaRDD<String> airportFile = sc.textFile(args[0]).flatMap(
                s -> Arrays.stream(s.split("\n")).iterator()
        );
        String airportHeader = airportFile.first();

        Map<String, String> stringAirportDataMap = airportFile
                .filter(s -> !s.equals(airportHeader))
                .mapToPair(s -> {
                        String[] fields = s.replace("\"", "").split(",", 2);
                        return new Tuple2<>(fields[CODE], fields[DESCRIPTION]);
                    })
                .collectAsMap();


        final Broadcast<Map<String, String>> airportsBroadcasted = sc.broadcast(stringAirportDataMap);
        JavaRDD<AirportData> result = airportData
                .map(s -> new AirportData(s, airportsBroadcasted.value()));

        result.saveAsTextFile(args[2]);
    }
}