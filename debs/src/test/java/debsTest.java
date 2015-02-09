import com.clearspring.analytics.stream.Counter;
import com.clearspring.analytics.stream.StreamSummary;
import com.clearspring.analytics.util.Pair;
import com.google.common.base.Splitter;
import extensions.cellId.CellIdFunction;
import extensions.maxK.MaxKTimeTransformerCopy;
import extensions.median.MedianAggregatorFactory;
import extensions.timeStamp.TimeStampFunction;
import org.junit.Test;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.config.SiddhiConfiguration;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.core.util.EventPrinter;
import org.wso2.siddhi.query.api.QueryFactory;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by sachini on 1/6/15.
 */
public class debsTest {


    @Test
    public void testTask1Query() throws InterruptedException {

        SiddhiConfiguration siddhiConfiguration = new SiddhiConfiguration();

        List<Class> extensions = new ArrayList<Class>();
        extensions.add(CellIdFunction.class);
        extensions.add(TimeStampFunction.class);
        extensions.add(MaxKTimeTransformerCopy.class);
        extensions.add(MedianAggregatorFactory.class);

        siddhiConfiguration.setSiddhiExtensions(extensions);

        SiddhiManager siddhiManager = new SiddhiManager(siddhiConfiguration);

        //07290D3599E7A0D62097A346EFCC1FB5,E7750A37CAB07D0DFF0AF7E3573AC141,2013-01-01 00:00:00,2013-01-01 00:02:00,120,0.44,-73.956528,40.716976,-73.962440,40.715008,CSH,3.50,0.50,0.50,0.00,0.0
        InputHandler inputHandler = siddhiManager.defineStream(
                QueryFactory.createStreamDefinition().name("taxi_trips").
                        attribute("medallion", Attribute.Type.STRING).attribute("hack_license", Attribute.Type.STRING).
                        attribute("pickup_datetime", Attribute.Type.STRING).attribute("dropoff_datetime", Attribute.Type.STRING).
                        attribute("trip_time_in_secs", Attribute.Type.INT).attribute("trip_distance", Attribute.Type.FLOAT).
                        attribute("pickup_longitude", Attribute.Type.FLOAT).attribute("pickup_latitude", Attribute.Type.FLOAT).
                        attribute("dropoff_longitude", Attribute.Type.FLOAT).attribute("dropoff_latitude", Attribute.Type.FLOAT).
                        attribute("payment_type", Attribute.Type.STRING).attribute("fare_amount", Attribute.Type.FLOAT).
                        attribute("surcharge", Attribute.Type.FLOAT).attribute("mta_tax", Attribute.Type.FLOAT).
                        attribute("tip_amount", Attribute.Type.FLOAT).attribute("tolls_amount", Attribute.Type.FLOAT).
                        attribute("total_amount", Attribute.Type.FLOAT)
        );


        String taskOne = "from taxi_trips select debs:cellId(pickup_longitude,pickup_latitude) as startCellNo, debs:cellId(dropoff_longitude,dropoff_latitude) as endCellNo , " +
                "debs:getTimestamp(pickup_datetime) as pickup_datetime , debs:getTimestamp(dropoff_datetime) as dropoff_datetime insert into cell_based_taxi_trips;";

        siddhiManager.addQuery(taskOne);
        //filter the cell nos
        taskOne = "from cell_based_taxi_trips[not(startCellNo contains '-') and not(endCellNo contains '-')] select * insert into filtered_cell_based_taxi_trips;";

        siddhiManager.addQuery(taskOne);
        //window of 30 mis calculate count groupby   startCellNo, endCellNo  -> startCellNo, endCellNo,pickup_datetime, dropoff_datetime, count
        taskOne = "from filtered_cell_based_taxi_trips#window.externalTime(dropoff_datetime,30 min) select startCellNo,endCellNo, pickup_datetime, dropoff_datetime, count(*) as tripCount group by startCellNo,endCellNo insert into countStream for all-events;";

        siddhiManager.addQuery(taskOne);
        //topk -> from inputStream#transform.topK:getTopKLength(count, 10, 1) select *,   insert into topKOutStream;
        taskOne = "from countStream#transform.MaxK:getMaxK(tripCount, startCellNo, endCellNo, 10) select * insert into duplicate_outputStream";
        siddhiManager.addQuery(taskOne);

        //discard duplicates
        taskOne = "from duplicate_outputStream[duplicate == false] select pickup_datetime, dropoff_datetime, startCell1 ,endCell1, startCell2,endCell2," +
                "startCell3 ,endCell3, startCell4, endCell4, startCell5, endCell5, startCell6, endCell6," +
                "startCell7 ,endCell7 , startCell8, endCell8, startCell9, endCell9, startCell10, endCell10 insert into outputStream";
        siddhiManager.addQuery(taskOne);




        siddhiManager.addCallback("outputStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
            }
        });

//       //The profit that originates from an area is computed by calculating the median fare + tip for trips that started in the area and ended within the last 15 minutes
//
//        String taskTwo = "from taxi_trips select debs:cellId(pickup_longitude,pickup_latitude) as startCellNo, debs:cellId(dropoff_longitude,dropoff_latitude) as endCellNo , " +
//                "debs:getTimestamp(pickup_datetime) as pickup_datetime, debs:getTimestamp(dropoff_datetime) as dropoff_datetime, fare_amount, tip_amount, medallion insert into cell_based_taxi_trips_2;";
//
//
//
//
//        //get profit
//        taskTwo = "from cell_based_taxi_trips_2#window.time(15 min) select custom:median(fare_amount+tip_amount) as profit, startCellNo, pickup_datetime, dropoff_datetime   group by startCellNo insert into profitStream for all-events";
//
//
//        //The number of empty taxis in an area is the sum of taxis that had a drop-off location in that area less than 30 minutes ago and had no following pickup yet.
//
//
//        siddhiManager.defineTable("define table emptyTaxiTable (medallion string, dropoff_datetime long, cellNo string) ");
//
//
//
//        // add events in last 30mins to table
//        taskTwo = "from cell_based_taxi_trips_2#window.time(30 min) select medallion , dropoff_datetime, endCellNo as cellNo  insert into emptyTaxiTable ";
//
//        //remove expired events(records before last 30 mins)  from table
//        taskTwo = "from cell_based_taxi_trips_2#window.time(30 min) select medallion , dropoff_datetime, endCellNo as cellNo delete emptyTaxiTable for expired-events on" +
//                " (medallion==emptyTaxiTable.medallion and  dropoff_datetime==emptyTaxiTable.dropoff_datetime) ";
//
//
//        //remove taxis which have a new pickup location
//        taskTwo = "from every e1 = cell_based_taxi_trips_2 -> e2 = cell_based_taxi_trips_2 [e1.medallion == medallion]  " +
//                "select e1.medallion as medallion,  e1.dropoff_datetime as dropoff_datetime " +
//                "insert into unavailable_taxis; " ;
//
//        taskTwo = "from unavailable_taxis delete emptyTaxiTable on (medallion==emptyTaxiTable.medallion and  dropoff_datetime==emptyTaxiTable.dropoff_datetime) ;";
//
//
//
//        //The profitability of an area is determined by dividing the area profit by the number of empty taxis in that area within the last 15 minutes.
//
//
//        //join median and empty taxis
//        taskTwo = "from profitStream#window.length(0) join emptyTaxiTable " +
//                "on profitStream.startCellNo == emptyTaxiTable.cellNo " +
//                "select profitStream.startCellNo as cellNo, profitStream.pickup_datetime as pickup_datetime , profitStream.dropoff_datetime as dropoff_datetime, " +
//                "profitStream.profit as profit , count(medallion) as emptyTaxis " +
//                "group by emptyTaxiTable.cellNo insert into profitRawData;";
//
//
//
//        ///get profit
//        taskTwo = "from profitRawData select cellNo, profit, emptyTaxis ,  pickup_datetime, dropoff_datetime, profit/emptyTaxis as profitt insert into finalProfitStream;";
//
//
//        taskTwo = "from finalProfitStream#transform.MaxK:getMaxK(profitt, cellNo, pickup_datetime, dropoff_datetime,10) select * insert into profitoutputStream";
//
//        taskTwo = "from profitoutputStream[!duplicate] select pickup_datetime, dropoff_datetime, " +
//                "profitable_cell_id_1, empty_taxies_in_cell_id_1, median_profit_in_cell_id_1, profitability_of_cell_1," +
//                "profitable_cell_id_2, empty_taxies_in_cell_id_2, median_profit_in_cell_id_2, profitability_of_cell_2," +
//                "profitable_cell_id_3, empty_taxies_in_cell_id_3, median_profit_in_cell_id_3, profitability_of_cell_3," +
//                "profitable_cell_id_4, empty_taxies_in_cell_id_4, median_profit_in_cell_id_4, profitability_of_cell_4," +
//                "profitable_cell_id_5, empty_taxies_in_cell_id_5, median_profit_in_cell_id_5, profitability_of_cell_5," +
//                "profitable_cell_id_6, empty_taxies_in_cell_id_6, median_profit_in_cell_id_6, profitability_of_cell_6," +
//                "profitable_cell_id_7, empty_taxies_in_cell_id_7, median_profit_in_cell_id_7, profitability_of_cell_7," +
//                "profitable_cell_id_8, empty_taxies_in_cell_id_8, median_profit_in_cell_id_8, profitability_of_cell_8," +
//                "profitable_cell_id_9, empty_taxies_in_cell_id_9, median_profit_in_cell_id_9, profitability_of_cell_9," +
//                "profitable_cell_id_10, empty_taxies_in_cell_id_10, median_profit_in_cell_id_10, profitability_of_cell_10," +
//                "insert into outputStream";


//
        List<Object[]> hh = readFromCsv("/home/sachini/dev/debs/sorted_data.csv");

        System.out.println("##############Done Reading");
        for(Object[] obj : hh) {
            inputHandler.send(System.currentTimeMillis(), obj);
        };


        Thread.sleep(2000000);
//      siddhiManager.shutdown();

    }



    @Test
    public void testTaskTwoQuery() throws InterruptedException {

        SiddhiConfiguration siddhiConfiguration = new SiddhiConfiguration();

        List<Class> extensions = new ArrayList<Class>();
        extensions.add(CellIdFunction.class);
        extensions.add(TimeStampFunction.class);
        extensions.add(MaxKTimeTransformerCopy.class);
        extensions.add(MedianAggregatorFactory.class);

        siddhiConfiguration.setSiddhiExtensions(extensions);

        SiddhiManager siddhiManager = new SiddhiManager(siddhiConfiguration);

        //07290D3599E7A0D62097A346EFCC1FB5,E7750A37CAB07D0DFF0AF7E3573AC141,2013-01-01 00:00:00,2013-01-01 00:02:00,120,0.44,-73.956528,40.716976,-73.962440,40.715008,CSH,3.50,0.50,0.50,0.00,0.0
        InputHandler inputHandler = siddhiManager.defineStream(
                QueryFactory.createStreamDefinition().name("taxi_trips").
                        attribute("medallion", Attribute.Type.STRING).attribute("hack_license", Attribute.Type.STRING).
                        attribute("pickup_datetime", Attribute.Type.STRING).attribute("dropoff_datetime", Attribute.Type.STRING).
                        attribute("trip_time_in_secs", Attribute.Type.INT).attribute("trip_distance", Attribute.Type.FLOAT).
                        attribute("pickup_longitude", Attribute.Type.FLOAT).attribute("pickup_latitude", Attribute.Type.FLOAT).
                        attribute("dropoff_longitude", Attribute.Type.FLOAT).attribute("dropoff_latitude", Attribute.Type.FLOAT).
                        attribute("payment_type", Attribute.Type.STRING).attribute("fare_amount", Attribute.Type.FLOAT).
                        attribute("surcharge", Attribute.Type.FLOAT).attribute("mta_tax", Attribute.Type.FLOAT).
                        attribute("tip_amount", Attribute.Type.FLOAT).attribute("tolls_amount", Attribute.Type.FLOAT).
                        attribute("total_amount", Attribute.Type.FLOAT)
        );



        //The profit that originates from an area is computed by calculating the median fare + tip for trips that started in the area and ended within the last 15 minutes

        String taskTwo = "from taxi_trips select debs:cellId(pickup_longitude,pickup_latitude) as startCellNo, debs:cellId(dropoff_longitude,dropoff_latitude) as endCellNo , " +
                "debs:getTimestamp(pickup_datetime) as pickup_datetime, debs:getTimestamp(dropoff_datetime) as dropoff_datetime, fare_amount, tip_amount, medallion insert into cell_based_taxi_trips_2;";

        siddhiManager.addQuery(taskTwo);

        taskTwo = "from cell_based_taxi_trips_2[not(startCellNo contains '-') and not(endCellNo contains '-')] select * insert into filtered_cell_based_taxi_trips;";
        siddhiManager.addQuery(taskTwo);

        //get profit
        taskTwo = "from filtered_cell_based_taxi_trips#window.externalTime(dropoff_datetime,15 min) select custom:median(fare_amount+tip_amount) as profit, startCellNo, pickup_datetime, dropoff_datetime   group by startCellNo insert into profitStream for all-events";

         siddhiManager.addQuery(taskTwo);

        //The number of empty taxis in an area is the sum of taxis that had a drop-off location in that area less than 30 minutes ago and had no following pickup yet.


        siddhiManager.defineTable("define table emptyTaxiTable (medallion string, dropoff_datetime long, cellNo string) ");



        // add events in last 30mins to table
        taskTwo = "from filtered_cell_based_taxi_trips select medallion , dropoff_datetime, endCellNo as cellNo  insert into emptyTaxiTable ";
          siddhiManager.addQuery(taskTwo);

        //remove expired events(records before last 30 mins)  from table
        taskTwo = "from filtered_cell_based_taxi_trips#window.externalTime(dropoff_datetime,30 min) select medallion , dropoff_datetime, endCellNo as cellNo delete emptyTaxiTable for expired-events on" +
                " (medallion==emptyTaxiTable.medallion and  dropoff_datetime==emptyTaxiTable.dropoff_datetime) ";
         siddhiManager.addQuery(taskTwo);

        //remove taxis which have a new pickup location
        taskTwo = "from every e1 = filtered_cell_based_taxi_trips -> e2 = filtered_cell_based_taxi_trips [e1.medallion == medallion]  " +
                "select e1.medallion as medallion,  e1.dropoff_datetime as dropoff_datetime " +
                "insert into unavailable_taxis; " ;

        siddhiManager.addQuery(taskTwo);

        taskTwo = "from unavailable_taxis delete emptyTaxiTable on (medallion==emptyTaxiTable.medallion and  dropoff_datetime==emptyTaxiTable.dropoff_datetime) ;";

         siddhiManager.addQuery(taskTwo);


        //The profitability of an area is determined by dividing the area profit by the number of empty taxis in that area within the last 15 minutes.


        //join median and empty taxis
        taskTwo = "from profitStream#window.length(0) join emptyTaxiTable " +
                "on profitStream.startCellNo == emptyTaxiTable.cellNo " +
                "select profitStream.startCellNo as cellNo, profitStream.pickup_datetime as pickup_datetime , profitStream.dropoff_datetime as dropoff_datetime, " +
                "profitStream.profit as profit , count(medallion) as emptyTaxis " +
                "group by emptyTaxiTable.cellNo insert into profitRawData;";

          siddhiManager.addQuery(taskTwo);


        ///get profit
        taskTwo = "from profitRawData select cellNo, profit, emptyTaxis ,  pickup_datetime, dropoff_datetime, profit/emptyTaxis as profitt insert into finalProfitStream;";
          siddhiManager.addQuery(taskTwo);

        taskTwo = "from finalProfitStream#transform.MaxK:getMaxK(profitt, cellNo, pickup_datetime, dropoff_datetime,10) select * insert into profitoutputStream";

        taskTwo = "from profitoutputStream[!duplicate] select pickup_datetime, dropoff_datetime, " +
                "profitable_cell_id_1, empty_taxies_in_cell_id_1, median_profit_in_cell_id_1, profitability_of_cell_1," +
                "profitable_cell_id_2, empty_taxies_in_cell_id_2, median_profit_in_cell_id_2, profitability_of_cell_2," +
                "profitable_cell_id_3, empty_taxies_in_cell_id_3, median_profit_in_cell_id_3, profitability_of_cell_3," +
                "profitable_cell_id_4, empty_taxies_in_cell_id_4, median_profit_in_cell_id_4, profitability_of_cell_4," +
                "profitable_cell_id_5, empty_taxies_in_cell_id_5, median_profit_in_cell_id_5, profitability_of_cell_5," +
                "profitable_cell_id_6, empty_taxies_in_cell_id_6, median_profit_in_cell_id_6, profitability_of_cell_6," +
                "profitable_cell_id_7, empty_taxies_in_cell_id_7, median_profit_in_cell_id_7, profitability_of_cell_7," +
                "profitable_cell_id_8, empty_taxies_in_cell_id_8, median_profit_in_cell_id_8, profitability_of_cell_8," +
                "profitable_cell_id_9, empty_taxies_in_cell_id_9, median_profit_in_cell_id_9, profitability_of_cell_9," +
                "profitable_cell_id_10, empty_taxies_in_cell_id_10, median_profit_in_cell_id_10, profitability_of_cell_10," +
                "insert into outputStream";





        siddhiManager.addCallback("finalProfitStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
            }
        });


        List<Object[]> hh = readFromCsv("/home/sachini/dev/debs/sorted_data.csv");

        System.out.println("##############Done Reading");
        for(Object[] obj : hh) {
            inputHandler.send(System.currentTimeMillis(), obj);
        };


        Thread.sleep(2000000);
//      siddhiManager.shutdown();

    }











//    public static void main(String[] args) {
//        StreamSummary<Integer> topk = new StreamSummary<Integer>(1000);
//        int count = 0;
//        for (int i=0; i<1000;i++) {
//            topk.offer(i%17);
//            count++;
//        }
//
//
//        Pair<Boolean, Integer> g = topk.offerReturnAll(5, -10);
////        topk.
//        count = 0;
//        List<Counter<Integer>> counters = topk.topK(5);
//        for (Counter counter : counters) {
//            System.out.println((String.format(" %s %d %d",
//                    counter.getItem(), counter.getCount(),
//                    counter.getError())));
//        }
//    }




    public List<Object[]> readFromCsv(String fileName){
        BufferedReader br;
        long start = System.currentTimeMillis();
        System.out.println("Start Processing Data");
        List<Object[]> objectsList = new ArrayList<Object[]>();
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

                Object[] eventData = new Object[]{medallion, hack_license , pickup_datetime, dropoff_datetime, Integer.parseInt(trip_time_in_secs), Float.parseFloat(trip_distance),
                        Float.parseFloat(pickup_longitude), Float.parseFloat(pickup_latitude), Float.parseFloat(dropoff_longitude), Float.parseFloat(dropoff_latitude),
                        payment_type, Float.parseFloat(fare_amount), Float.parseFloat(surcharge), Float.parseFloat(mta_tax), Float.parseFloat(tip_amount), Float.parseFloat(tolls_amount),
                        Float.parseFloat(total_amount)};

                objectsList.add(eventData);

                line = br.readLine();
                count++;
            }


        } catch (FileNotFoundException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } catch (Throwable e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }

        return objectsList;

    }



}
