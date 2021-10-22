package ru.evistix28.parallels;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;

public class AirportReader {
    public static final int DEST_AIRPORT_ID_POS = 0;
    public static final int AIRPORT_NAME_POS = 1;
    public static final int LIMIT_FOR_SPLIT = 1;

    private final int airportID;
    private final Text airportName;


    public int getKey() {return airportID;}
    public Text getAirportName() {return airportName;}



    public AirportReader(String value) {
        String[] dataPair = value.split(Constants.DATA_SEPARATOR, LIMIT_FOR_SPLIT);
        System.out.println(dataPair[DEST_AIRPORT_ID_POS]);
        airportID = Integer.parseInt(dataPair[DEST_AIRPORT_ID_POS].replaceAll(Constants.QUOTES, Constants.EMPTY_STRING));
        airportName = new Text(dataPair[AIRPORT_NAME_POS].replaceAll(Constants.QUOTES, Constants.EMPTY_STRING));
    }




}
