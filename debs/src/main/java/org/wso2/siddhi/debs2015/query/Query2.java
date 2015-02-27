package org.wso2.siddhi.debs2015.query;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.config.SiddhiConfiguration;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.core.util.EventPrinter;
import org.wso2.siddhi.debs2015.extensions.cellId.CellIdFunction;
import org.wso2.siddhi.debs2015.extensions.maxK.MaxKTimeTransformerCopy;
import org.wso2.siddhi.debs2015.extensions.median.MedianAggregatorFactory;
import org.wso2.siddhi.debs2015.extensions.timeStamp.TimeStampFunction;
import org.wso2.siddhi.debs2015.util.Config;
import org.wso2.siddhi.debs2015.util.Constants;
import org.wso2.siddhi.debs2015.util.EventLogger;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.QueryFactory;

import com.google.common.base.Splitter;

/**
 * 
 */


public class Query2 {
	private static Splitter splitter = Splitter.on(',');
	private static LinkedList<String> aggregateInputList = new LinkedList<String>();
	private static LinkedList<String> aggregateOutputList = new LinkedList<String>();
	private static LinkedList<Object> eventBufferList = new LinkedList<Object>();
	private static final int INPUT_INJECTION_TIMESTAMP_FIELD = 17;	
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
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
    	                        attribute("medallion", Attribute.Type.STRING).
    	                        attribute("hack_license", Attribute.Type.STRING).
    	                        attribute("pickup_datetime", Attribute.Type.STRING).
    	                        attribute("dropoff_datetime", Attribute.Type.STRING).
    	                        attribute("trip_time_in_secs", Attribute.Type.INT).
    	                        attribute("trip_distance", Attribute.Type.FLOAT).
    	                        attribute("pickup_longitude", Attribute.Type.FLOAT).
    	                        attribute("pickup_latitude", Attribute.Type.FLOAT).
    	                        attribute("dropoff_longitude", Attribute.Type.FLOAT).
    	                        attribute("dropoff_latitude", Attribute.Type.FLOAT).
    	                        attribute("payment_type", Attribute.Type.STRING).
    	                        attribute("fare_amount", Attribute.Type.FLOAT).
    	                        attribute("surcharge", Attribute.Type.FLOAT).
    	                        attribute("mta_tax", Attribute.Type.FLOAT).
    	                        attribute("tip_amount", Attribute.Type.FLOAT).
    	                        attribute("tolls_amount", Attribute.Type.FLOAT).
    	                        attribute("total_amount", Attribute.Type.FLOAT).
    	                        attribute("iij_timestamp", Attribute.Type.LONG)			//This is an additional field used to indicate the time when the event has been injected to the query network.
    	        );
    	
    	        //The profit that originates from an area is computed by calculating the median fare + tip for trips that started in the area and ended within the last 15 minutes
    	        //Output stream is of the format : cell_based_taxi_trips_2(startCellNo, endCellNo, pickup_datetime, dropoff_datetime, fare_amount, tip_amount, medallion)
    	        String taskTwo = "from taxi_trips select debs:cellId(pickup_longitude,pickup_latitude) as startCellNo, debs:cellId(dropoff_longitude,dropoff_latitude) as endCellNo , " +
    	                "debs:getTimestamp(pickup_datetime) as pickup_datetime, debs:getTimestamp(dropoff_datetime) as dropoff_datetime, fare_amount, tip_amount, medallion, iij_timestamp insert into cell_based_taxi_trips_2;";
    	
    	        siddhiManager.addQuery(taskTwo);
    	
    	        //filter the outlier cells
    	        //In the second query, the cell ID must be in the range 0.0 to 600.600. The debs:cellId() function inserts '-' to the cell ID.
    	        //Output stream from taskOne is of the format : cell_based_taxi_trips2(startCellNo, endCellNo, pickup_datetime, dropoff_datetime, fare_amount, tip_amount, medallion)
    	        //+Miyuru: This query might be not needed as described in CellIdFunction
    	        taskTwo = "from cell_based_taxi_trips_2[not(startCellNo contains '-') and not(endCellNo contains '-')] select * insert into filtered_cell_based_taxi_trips;";
    	        siddhiManager.addQuery(taskTwo);
    	
    	        //get profit
    	        //Output stream is of the format : profitStream(profit, startCellNo, pickup_datetime, dropoff_datetime)
    	        //taskTwo = "from filtered_cell_based_taxi_trips#window.externalTime(dropoff_datetime,15 min) select custom:median(fare_amount+tip_amount) as profit, startCellNo, pickup_datetime, dropoff_datetime, iij_timestamp group by startCellNo insert into profitStream for all-events";
    	        taskTwo = "from filtered_cell_based_taxi_trips#window.externalTime(pickup_datetime,15 min) select custom:median(fare_amount+tip_amount) as profit, startCellNo, pickup_datetime, dropoff_datetime, iij_timestamp group by startCellNo insert into profitStream for all-events";
    	
    	        siddhiManager.addQuery(taskTwo);
    	        
    	        //The number of empty taxis in an area is the sum of taxis that had a drop-off location in that area less than 30 minutes ago and had no following pickup yet.
    	
    	        siddhiManager.defineTable("define table emptyTaxiTable (medallion string, dropoff_datetime long, cellNo string) ");
    	
    	        // add events in last 30mins to table
    	        taskTwo = "from filtered_cell_based_taxi_trips select medallion , dropoff_datetime, endCellNo as cellNo  insert into emptyTaxiTable ";
    	          siddhiManager.addQuery(taskTwo);
    	
    	          
    	        //remove expired events(records before the last 30 mins) from table
    	        taskTwo = "from filtered_cell_based_taxi_trips#window.externalTime(dropoff_datetime,30 min) select medallion, dropoff_datetime, endCellNo as cellNo delete emptyTaxiTable for expired-events on" +
    	                " (medallion==emptyTaxiTable.medallion and  dropoff_datetime==emptyTaxiTable.dropoff_datetime) ";
    	         siddhiManager.addQuery(taskTwo);
    	
    	        //--------------------------------------------------------------------------------------------------------------------------------
//    	        //remove taxis which have a new pickup location
//    	        taskTwo = "from every e1 = filtered_cell_based_taxi_trips -> e2 = filtered_cell_based_taxi_trips [e1.medallion == medallion]  " +
//    	                "select e1.medallion as medallion,  e1.dropoff_datetime as dropoff_datetime " +
//    	                "insert into unavailable_taxis; " ;
//    	
//    	        siddhiManager.addQuery(taskTwo);
//    	
//    	        taskTwo = "from unavailable_taxis delete emptyTaxiTable on (medallion==emptyTaxiTable.medallion and  dropoff_datetime==emptyTaxiTable.dropoff_datetime) ;";
//    	
//    	         siddhiManager.addQuery(taskTwo);
                //--------------------------------------------------------------------------------------------------------------------------------
    	
    	        //join median and empty taxis
    	        taskTwo = "from profitStream#window.length(0) join emptyTaxiTable " +
    	                "on profitStream.startCellNo == emptyTaxiTable.cellNo " +
    	                "select profitStream.startCellNo as cellNo, profitStream.pickup_datetime as pickup_datetime , profitStream.dropoff_datetime as dropoff_datetime, " +
    	                "profitStream.profit as profit , count(medallion) as emptyTaxiCount " +
    	                "group by emptyTaxiTable.cellNo insert into profitRawData;";
    	
    	          siddhiManager.addQuery(taskTwo);
    	          
    	          
//                  final EventLogger logger = new EventLogger("q7.txt");
//                  logger.start();
//      	        
////                  siddhiManager.addCallback("countStream", new StreamCallback() {
////                      @Override
////                      public void receive(Event[] events) {
////                          EventPrinter.print(events);
////                      	//logger2.addEvents(events);
////                      }
////                });
//                
//                siddhiManager.addCallback("profitRawData", new StreamCallback() {
//                      @Override
//                      public void receive(Event[] events) {
//                          //EventPrinter.print(events);
//                      	logger.addEvents(events);
//                      }
//                });
    	          
    	          
    	
    	        //The profitability of an area is determined by dividing the area profit by the number of empty taxis in that area within the last 15 minutes.    	
    	        //get profit
    	        taskTwo = "from profitRawData select cellNo, profit, emptyTaxiCount,  pickup_datetime, dropoff_datetime, profit/emptyTaxiCount as profit_per_taxi insert into finalProfitStream;";
    	          siddhiManager.addQuery(taskTwo);
    	        //                                               getMaxK(tripCount, startCellNo, endCellNo, 10, iij_timestamp)
    	        taskTwo = "from finalProfitStream#transform.MaxK:getMaxK(profit_per_taxi, cellNo, pickup_datetime, dropoff_datetime,10) select * insert into profitoutputStream";
    	
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
    	
    	
    	        siddhiManager.addCallback("outputStream", new StreamCallback() {
                	long count = 1;
                	long totalLatency = 0;
                	long latencyFromBegining = 0;
                	long latencyWithinEventCountWindow = 0;
                    long startTime = System.currentTimeMillis();
                    long timeDifferenceFromStart = 0;
                    long timeDifference = 0; //This is the time difference for this time window.
                    long currentTime = 0;
                    long prevTime = 0;
                    long latency = 0;
                    
                    @Override
                    public void receive(Event[] events) {
                    	for(Event evt : events){         
                            currentTime = System.currentTimeMillis();
                            long eventOriginationTime = (Long)evt.getData()[22];
                            latency = currentTime - eventOriginationTime;
                            latencyWithinEventCountWindow += latency;
                            latencyFromBegining += latency;
                            
                            if (count % Constants.STATUS_REPORTING_WINDOW_OUTPUT == 0) {
                                timeDifferenceFromStart = currentTime - startTime;
                                timeDifference = currentTime - prevTime; 
                                //<time from start(ms)><time from start(s)><overall latency (ms/event)><latency in this time window (ms/event)><over all throughput (events/s)><throughput in this time window (events/s)>
                                aggregateOutputList.add(timeDifferenceFromStart + "," + Math.round(timeDifferenceFromStart/1000) + "," + Math.round(latencyFromBegining * 1.0d/count) + "," + Math.round(latencyWithinEventCountWindow * 1.0d/Constants.STATUS_REPORTING_WINDOW_OUTPUT) + "," + Math.round(count * 1000.0d/timeDifferenceFromStart) + "," + Math.round(Constants.STATUS_REPORTING_WINDOW_OUTPUT * 1000.0d/timeDifference) );
                                prevTime = currentTime;
                                latencyWithinEventCountWindow = 0;
                            }
                            count++;
                    	}
                    }
                });
    	
    		   	        
              startMonitoring();
              loadEventsFromFile(Config.getConfigurationInfo("org.wso2.siddhi.debs2015.dataset"));
              sendEventsFromQueue(inputHandler);
              
    	        try {
    	            Thread.sleep(2000000);
                } catch (InterruptedException e) {
    	            e.printStackTrace();
                }
	}
	
	/**
	 * This method logs the stream statistics. This is coded as a separate Java thread so that it will remove the overhead of disk access from the main thread.
	 */
    private static void startMonitoring() {
    	//Input data rate
    	Thread monitoringThreadInput = new Thread(){
    		FileWriter fw = null;
			BufferedWriter bw = null;
			
    		public void run(){
    			String record = null;
    			Date dNow = new Date();
    			SimpleDateFormat ft = new SimpleDateFormat ("yyyy-MM-dd.hh:mm:ss-a-zzz");
    			String timeStamp = ft.format(dNow);
    			String statisticsDir = Config.getConfigurationInfo("org.wso2.siddhi.debs2015.experiment.logdir");
        		try {
	                fw = new FileWriter(new File(statisticsDir + "/input-stats-" + timeStamp + ".csv").getAbsoluteFile());
	    			bw = new BufferedWriter(fw);
	    			bw.write("time from start(ms),time from start(s),aggregate throughput (events/s),throughput in this time window (events/s),percentage completed (%)\r\n");
                } catch (IOException e1) {
	                e1.printStackTrace();
                }
        		
    			while(true){
    				record = aggregateInputList.poll();
    				
    				if(record != null){
    					try {
	                        bw.write(record + "\r\n");
	                        bw.flush();
                        } catch (IOException e) {
	                        e.printStackTrace();
                        }
    				}
    				
    				//We sleep for half a second to avoid the CPU core being over utilized.
    				//Since this is just a monitoring thread, there should not be much harm for over all performance in doing so.
    				try {
	                    Thread.currentThread().sleep(Constants.MONITORING_THREAD_SLEEP_TIME);
                    } catch (InterruptedException e) {
	                    e.printStackTrace();
                    }
    			}
    		}
    	};
    	monitoringThreadInput.start();
    	
    	//Output 
    	Thread monitoringThreadOutput = new Thread(){
    		FileWriter fw = null;
			BufferedWriter bw = null;
			
    		public void run(){
    			String record = null;
    			Date dNow = new Date();
    			SimpleDateFormat ft = new SimpleDateFormat ("yyyy-MM-dd.hh:mm:ss-a-zzz");
    			String timeStamp = ft.format(dNow);
    			String statisticsDir = Config.getConfigurationInfo("org.wso2.siddhi.debs2015.experiment.logdir");
        		try {
	                fw = new FileWriter(new File(statisticsDir + "/output-stats-" + timeStamp + ".csv").getAbsoluteFile());
	    			bw = new BufferedWriter(fw);
	    			bw.write("time from start(ms),time from start(s), overall latency (ms/event), latency in this time window (ms/event), overall throughput(events/s), throughput in this time window (events/s)\r\n");
                } catch (IOException e1) {
	                e1.printStackTrace();
                }
        		
    			while(true){
    				record = aggregateOutputList.poll();
    				
    				if(record != null){
    					try {
	                        bw.write(record + "\r\n");
	                        bw.flush();
                        } catch (IOException e) {
	                        e.printStackTrace();
                        }
    				}
    				
    				try {
	                    Thread.currentThread().sleep(Constants.MONITORING_THREAD_SLEEP_TIME);
                    } catch (InterruptedException e) {
	                    e.printStackTrace();
                    }
    			}
    		}
    	};
    	monitoringThreadOutput.start();
    }
    
    /**
     * This method directly reads data from the data set file, constructs input events, and stores those on a queue.
     * @param inputHandler
     * @param fileName
     */
	public static void loadEventsFromFile(final String fileName) {
    	Thread inputDataLoaderThread = new Thread(){
			
    		public void run(){
                BufferedReader br;
        
                long count = 1;
                boolean dataLoadingFlag = true;
                long currentEventCount = 0;
                boolean incrementalLoadingFlag = Config.getConfigurationInfo("org.wso2.siddhi.debs2015.flags.incrementalloading").equals("true") ? true : false;
                
                if(!incrementalLoadingFlag){
                	System.out.println("Data set will be loaded to the memory completely.");
                }else{
                	System.out.println("Incremental data loading is performed.");
                }
                
                try {
                    br = new BufferedReader(new FileReader(fileName), 10 * 1024 * 1024);
                    String line = br.readLine();
                    while (line != null) {
                        	if(dataLoadingFlag){
                            	//We make an assumption here that we do not get empty strings due to missing values that may present in the input data set.
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
                            	
                                Object[] eventData = null;
                                
                                try{
                                eventData = new Object[]{		  medallion, 
                                                                  hack_license , 
                                                                  pickup_datetime, 
                                                                  dropoff_datetime, 
                                                                  Short.parseShort(trip_time_in_secs), 
                                                                  Float.parseFloat(trip_distance), //This can be represented by two bytes
                                                                  Float.parseFloat(pickup_longitude), 
                                                                  Float.parseFloat(pickup_latitude), 
                                                                  Float.parseFloat(dropoff_longitude), 
                                                                  Float.parseFloat(dropoff_latitude),
                                                                  Boolean.parseBoolean(payment_type),
                                                                  Float.parseFloat(fare_amount), //These currency values can be coded to two bytes 
                                                                  Float.parseFloat(surcharge), 
                                                                  Float.parseFloat(mta_tax), 
                                                                  Float.parseFloat(tip_amount), 
                                                                  Float.parseFloat(tolls_amount),
                                                                  Float.parseFloat(total_amount),
                                                                  0l}; //We need to attach the time when we are injecting an event to the query network. For that we have to set a separate field which will be populated when we are injecting an event to the input stream. 
                                }catch(NumberFormatException e){
                                	e.printStackTrace();
                                }

                                //We keep on accumulating data on to the event queue.
                                eventBufferList.add(eventData);
                                line = br.readLine();
                                count++;
                        }
                        
                        //This part of the code will get activated only if we enable the incremental data loading flag in the "debs2015.properties" configuration file.
                        if(incrementalLoadingFlag){
                            currentEventCount = eventBufferList.size();
                            
                            if(dataLoadingFlag && (currentEventCount > Constants.EVENT_BUFFER_CEIL)){
                            	dataLoadingFlag = false;
                            	System.out.println("Event loading deactivated");
                            }
                            
                            if((!dataLoadingFlag) && (currentEventCount < Constants.EVENT_BUFFER_FLOOR)){
                            	dataLoadingFlag = true;
                            	System.out.println("Event loading activated");
                            }
                        }
                    }
                    System.out.println("Total amount of events read : " + count);
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                } catch (Throwable e) {
                    e.printStackTrace();
                }
                System.out.println("Now exiting from data loader thread.");
    		}
    	};
    	inputDataLoaderThread.start();
    }
	
	public static void sendEventsFromQueue(final InputHandler inputHandler){
    	Thread inputDataPumpThread = new Thread(){
			
    		public void run(){
    			Object[] event = null;
                long count = 1;
                long timeDifferenceFromStart = 0;
                long timeDifference = 0; //This is the time difference for this time window.
                long currentTime = 0;
                long prevTime = 0;
                long startTime = System.currentTimeMillis();
                long cTime = 0;
                
    			while(true){
    				event = (Object[])eventBufferList.poll();
    				
    				if(event != null){
    					try {
    						cTime = System.currentTimeMillis();
    						event[INPUT_INJECTION_TIMESTAMP_FIELD]	= cTime; //This corresponds to the iij_timestamp
	                        inputHandler.send(cTime, event);
	                        
	                        if (count % Constants.STATUS_REPORTING_WINDOW_INPUT == 0) {
	                        	
	                            float percentageCompleted = ((float)count/ (Constants.EVENT_COUNT_PARTIAL_DATASET));
	                            currentTime = System.currentTimeMillis();
	                            timeDifferenceFromStart = (currentTime - startTime);
	                            timeDifference = currentTime - prevTime;
	                            //<time from start(ms)><time from start(s)><aggregate throughput (events/s)><throughput in this time window (events/s)><percentage completed (%)>
	                            aggregateInputList.add(timeDifferenceFromStart + "," + Math.round(timeDifferenceFromStart/1000) + "," + Math.round(count * 1000.0 / timeDifferenceFromStart) + "," + Math.round(Constants.STATUS_REPORTING_WINDOW_INPUT  * 1000.0/timeDifference) + "," + percentageCompleted);
	                            prevTime = currentTime;
	                        }
	                        count++;
                        } catch (InterruptedException e) {
	                        e.printStackTrace();
                        }
    				}
    			}
    		}
    	};
    	inputDataPumpThread.start();
	}
}
