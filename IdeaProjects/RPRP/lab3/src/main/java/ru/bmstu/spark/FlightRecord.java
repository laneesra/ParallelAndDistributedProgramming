package ru.bmstu.spark;

import java.io.Serializable;

public class FlightRecord implements Serializable {
    private String originAirportId;
    private String destAirportId;
    private double arrDelay;
    private boolean cancelled;

    public FlightRecord(String originAirportId, String destAirportId, String arrDelay, String cancelled) {
        this.originAirportId = originAirportId;
        this.destAirportId = destAirportId;

        try {
            this.arrDelay = Double.parseDouble(arrDelay);
        } catch (NumberFormatException e) {
            this.arrDelay = -1;
        }

        try {
            this.cancelled = Double.parseDouble(cancelled) == 1;
        } catch (NumberFormatException e) {
            this.arrDelay = 0;
        }
    }

    public String getOriginAirportId() {
        return originAirportId;
    }

    public String getDestAirportId() {
        return destAirportId;
    }

    public double getArrDelay() {
        return arrDelay;
    }

    public long isLateOrCanceled() {
        if (cancelled || arrDelay > 0) {
            return 1;
        } else {
            return 0;
        }
    }
}
