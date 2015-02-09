package util;

import com.google.common.base.Splitter;
import com.sun.org.apache.bcel.internal.generic.FLOAD;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.query.api.QueryFactory;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;

/**
 * Created by sachini on 1/22/15.
 */
public class CsvReader {

    private Splitter splitter = Splitter.on(',');

    public void readFromCsv(String fileName){
        BufferedReader br;
        long start = System.currentTimeMillis();
        System.out.println("Start Processing Data");

        Splitter splitter = Splitter.on(',');

        long count = 1;
        try {
            br = new BufferedReader(new FileReader(fileName), 10 * 1024 * 1024);
            String line = br.readLine();
            while (line != null) {
                Iterator<String> dataStrIterator = splitter.split(line).iterator();
                String medallion = dataStrIterator.next();
                String hack_license = dataStrIterator.next();
                String pickup_datetime = dataStrIterator.next();
                String dropoff_datetime = dataStrIterator.next();
                String trip_time_in_secs = dataStrIterator.next();
                String trip_distance = dataStrIterator.next();
                String pickup_longitude = dataStrIterator.next();
                String pickup_latitude = dataStrIterator.next();
                String dropoff_longitude = dataStrIterator.next();
                String dropoff_latitude = dataStrIterator.next();
                String payment_type = dataStrIterator.next();
                String fare_amount = dataStrIterator.next();
                String surcharge = dataStrIterator.next();
                String mta_tax = dataStrIterator.next();
                String tip_amount = dataStrIterator.next();
                String tolls_amount = dataStrIterator.next();
                String total_amount = dataStrIterator.next();

                Object[] eventData = new Object[]{medallion, hack_license , pickup_datetime, dropoff_datetime, Integer.parseInt(trip_time_in_secs), Integer.parseInt(trip_distance),
                         Float.parseFloat(pickup_longitude), Float.parseFloat(pickup_latitude), Float.parseFloat(dropoff_longitude), Float.parseFloat(dropoff_latitude),
                        payment_type, Float.parseFloat(fare_amount), Float.parseFloat(surcharge), Float.parseFloat(mta_tax), Float.parseFloat(tip_amount), Float.parseFloat(tolls_amount),
                        Float.parseFloat(total_amount)};



                line = br.readLine();
                count++;
            }


        } catch (FileNotFoundException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } catch (Throwable e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }

    }
}
