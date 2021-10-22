package ru.evistix28.parallels;

public class FlightReader {

    public static final int DEST_AIRPORT_ID_POS = 14;
    public static final int FLIGHT_DELAY_POS = 18;

    private final int airportID;
    private final float flightDelay;

    public int getKey() {return airportID;}
    public float getFlightDelay() {return flightDelay;}

    public FlightReader(String value) {
        String[] data = value.split(Constants.DATA_SEPARATOR);
        airportID = Integer.parseInt(data[DEST_AIRPORT_ID_POS]);
        flightDelay = Float.parseFloat(data[FLIGHT_DELAY_POS]);
        
    }
}
