package ru.evistix28.parallels;

public class FlightReader {

    public static final int DEST_AIRPORT_ID_POS = 14;
    public static final int FLIGHT_DELAY_POS = 18;
    public static final String EMPTY_STRING = "";


    private final int airportID;
    private final int flightDelay;

    public FlightReader(String value) {
        String[] data = value.split(AirportReader.DATA_SEPARATOR);
        airportID = Integer.parseInt(data[DEST_AIRPORT_ID_POS].replaceAll(AirportReader.QUOTES, EMPTY_STRING));
        flightDelay = Integer.parseInt()
    }
}
