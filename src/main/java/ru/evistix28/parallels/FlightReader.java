package ru.evistix28.parallels;



import java.io.IOException;

public class FlightReader {

    public static final int DEST_AIRPORT_ID_POS = 14;
    public static final int FLIGHT_DELAY_POS = 18;

    private final int airportID;
    private final float flightDelay;

    public int getKey() {return airportID;}
    public float getFlightDelay() {return flightDelay;}

    public FlightReader(String value) throws IOException {
        String[] data = value.split(Constants.DATA_SEPARATOR);
        airportID = Integer.parseInt(data[DEST_AIRPORT_ID_POS]);
        if (data[FLIGHT_DELAY_POS] != Constants.EMPTY_STRING)
            flightDelay = Float.parseFloat(data[FLIGHT_DELAY_POS]);
        else
            flightDelay = Constants.ZERO_FLOAT;
    }
}
