package ru.evistix28.parallels;

import org.apache.hadoop.io.Text;

public class AirPortReader {
    public static final int DEST_AIRPORT_ID_POS = 0;
    public static final int AIRPORT_NAME_POS = 1;
    public static final int DATA_SEPARATE;

    private final int airportID;
    private final Text airportName;

    public AirPortReader(int id, Text name) {
        airportID = id;
        airportName = name;
    }

    public int getKey() {return airportID;}
    public Text getAirportName() {return airportName;}

    public AirPortReader(String value) {
        String[] dataPair = value.split(",", 1);
    }




}
