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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

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
import org.wso2.siddhi.query.api.QueryFactory;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.google.common.base.Splitter;


public class Query1 {
	//private static final Logger logger = Logger.getLogger(Query1.class);
	private static Splitter splitter = Splitter.on(',');
	private static LinkedList<String> aggregateInputList = new LinkedList<String>();
	private static LinkedList<String> aggregateOutputList = new LinkedList<String>();
	//LinkedBlockingQueue
	private static LinkedBlockingQueue<Object> eventBufferList = new LinkedBlockingQueue<Object>();
	private static final int INPUT_INJECTION_TIMESTAMP_FIELD = 17;
		
	/**
	 * @param args
	 */
	public static void main(String[] args) {
			System.out.println("Started experiment at : " + System.currentTimeMillis());
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
                            attribute("medallion", Attribute.Type.STRING).			//an md5sum of the identifier of the taxi - vehicle bound
                            attribute("hack_license", Attribute.Type.STRING).		//an md5sum of the identifier for the taxi license
                            attribute("pickup_datetime", Attribute.Type.STRING).	//time when the passenger(s) were picked up
                            attribute("dropoff_datetime", Attribute.Type.STRING).	//time when the passenger(s) were dropped off
                            attribute("trip_time_in_secs", Attribute.Type.INT).		//duration of the trip (in seconds)
                            attribute("trip_distance", Attribute.Type.FLOAT).		//trip distance in miles
                            attribute("pickup_longitude", Attribute.Type.FLOAT).	//longitude coordinate of the pickup location
                            attribute("pickup_latitude", Attribute.Type.FLOAT).		//latitude coordinate of the pickup location
                            attribute("dropoff_longitude", Attribute.Type.FLOAT).	//longitude coordinate of the drop-off location
                            attribute("dropoff_latitude", Attribute.Type.FLOAT).	//latitude coordinate of the drop-off location
                            attribute("payment_type", Attribute.Type.STRING).		//the payment method - credit card or cash
                            attribute("fare_amount", Attribute.Type.FLOAT).			//fare amount in dollars
                            attribute("surcharge", Attribute.Type.FLOAT).			//surcharge in dollars
                            attribute("mta_tax", Attribute.Type.FLOAT).				//tax in dollars
                            attribute("tip_amount", Attribute.Type.FLOAT).			//tip in dollars
                            attribute("tolls_amount", Attribute.Type.FLOAT).		//bridge and tunnel tolls in dollars
                            attribute("total_amount", Attribute.Type.FLOAT).		//total paid amount in dollars
                            attribute("iij_timestamp", Attribute.Type.LONG)			//This is an additional field used to indicate the time when the event has been injected to the query network.
            );
    
            //Output stream from task one is of the format : cell_based_taxi_trips(startCellNo, endCellNo, pickup_datetime, dropoff_datetime, iij_timestamp)
            String task = "from taxi_trips select debs:cellId(pickup_longitude,pickup_latitude) as startCellNo, debs:cellId(dropoff_longitude,dropoff_latitude) as endCellNo , " +
                    "debs:getTimestamp(pickup_datetime) as pickup_datetime , debs:getTimestamp(dropoff_datetime) as dropoff_datetime, iij_timestamp insert into cell_based_taxi_trips;";
    
            siddhiManager.addQuery(task);
            //filter the outlier cells
            //In the first query, the cell ID must be in the range 0.0 to 300.300. The debs:cellId() function inserts '-' to the cell ID.
            //Output stream from taskOne is of the format : cell_based_taxi_trips(startCellNo, endCellNo, pickup_datetime, dropoff_datetime)
            //+Miyuru: This query might be not needed as described in CellIdFunction
            task = "from cell_based_taxi_trips[not(startCellNo contains '-') and not(endCellNo contains '-')] select * insert into filtered_cell_based_taxi_trips;";
    
            siddhiManager.addQuery(task);
            //window of 30 min calculate count groupby   startCellNo, endCellNo  -> startCellNo, endCellNo, pickup_datetime, dropoff_datetime, count
            //task = "from filtered_cell_based_taxi_trips#window.externalTime(dropoff_datetime,30 min) select startCellNo,endCellNo, pickup_datetime, dropoff_datetime, count(*) as tripCount, iij_timestamp group by startCellNo,endCellNo insert into countStream for all-events;";
            task = "from filtered_cell_based_taxi_trips#window.externalTime(dropoff_datetime,30 min) select startCellNo, endCellNo, pickup_datetime, dropoff_datetime, count(*) as tripCount, iij_timestamp group by startCellNo,endCellNo insert into countStream for all-events;";
            siddhiManager.addQuery(task);
            
            task = "from countStream#transform.MaxK:getMaxK(tripCount, startCellNo, endCellNo, 10, iij_timestamp) select * insert into duplicate_outputStream";
            siddhiManager.addQuery(task);
                
            
            //discard duplicates
            task = "from duplicate_outputStream[duplicate == false] select pickup_datetime, dropoff_datetime, startCell1 ,endCell1, startCell2, endCell2," +
                    "startCell3 ,endCell3, startCell4, endCell4, startCell5, endCell5, startCell6, endCell6," +
                    "startCell7 ,endCell7 , startCell8, endCell8, startCell9, endCell9, startCell10, endCell10, iij_timestamp insert into outputStream";
            siddhiManager.addQuery(task);
    
            
            //Note: If less than 10 routes can be identified within the last 30 min, then NULL is to be output for all routes that lack data.
            
            //The attribute “delay” captures the time delay between reading the input event that triggered the output and the time when the output 
            //is produced. Participants must determine the delay using the current system time right after reading the input and right before writing 
            //the output. This attribute will be used in the evaluation of the submission.
    
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
//                	for(Event evt : events){         
//                        currentTime = System.currentTimeMillis();
//                        long eventOriginationTime = (Long)evt.getData()[22];
//                        latency = currentTime - eventOriginationTime;
//                        latencyWithinEventCountWindow += latency;
//                        latencyFromBegining += latency;
//                        
//                        if (count % Constants.STATUS_REPORTING_WINDOW_OUTPUT == 0) {
//                            timeDifferenceFromStart = currentTime - startTime;
//                            timeDifference = currentTime - prevTime; 
//                            //<time from start(ms)><time from start(s)><overall latency (ms/event)><latency in this time window (ms/event)><over all throughput (events/s)><throughput in this time window (events/s)>
//                            aggregateOutputList.add(timeDifferenceFromStart + "," + Math.round(timeDifferenceFromStart/1000) + "," + Math.round(latencyFromBegining * 1.0d/count) + "," + Math.round(latencyWithinEventCountWindow * 1.0d/Constants.STATUS_REPORTING_WINDOW_OUTPUT) + "," + Math.round(count * 1000.0d/timeDifferenceFromStart) + "," + Math.round(Constants.STATUS_REPORTING_WINDOW_OUTPUT * 1000.0d/timeDifference) );
//                            prevTime = currentTime;
//                            latencyWithinEventCountWindow = 0;
//                        }
//                        count++;
//                	}
                }
            });
            
            //startMonitoring();
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
     * This method directly reads data from the data set file, constructs input events, and sends those to the query network.
     * @param inputHandler
     * @param fileName
     */
	public static void sendEventsFromFile(InputHandler inputHandler, String fileName) {
        BufferedReader br;

        long count = 1;
        long timeDifferenceFromStart = 0;
        long timeDifference = 0; //This is the time difference for this time window.
        long currentTime = 0;
        long prevTime = 0;
        long startTime = System.currentTimeMillis();
        
        try {
            br = new BufferedReader(new FileReader(fileName), 10 * 1024 * 1024);
            String line = br.readLine();
            while (line != null) {
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
                eventData = new Object[]{medallion, 
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
                                                  Float.parseFloat(fare_amount), //These currency values can be coded to  two bytes 
                                                  Float.parseFloat(surcharge), 
                                                  Float.parseFloat(mta_tax), 
                                                  Float.parseFloat(tip_amount), 
                                                  Float.parseFloat(tolls_amount),
                                                  Float.parseFloat(total_amount)};
                }catch(NumberFormatException e){
                	e.printStackTrace();
                }

                inputHandler.send(System.currentTimeMillis(), eventData);

                if (count % Constants.STATUS_REPORTING_WINDOW_INPUT == 0) {
                    float percentageCompleted = (count / Constants.EVENT_COUNT_PARTIAL_DATASET);
                    currentTime = System.currentTimeMillis();
                    timeDifferenceFromStart = (currentTime - startTime);
                    timeDifference = currentTime - prevTime;
                    //<time from start(ms)><time from start(s)><aggregate throughput (events/s)><throughput in this time window (events/s)><percentage completed (%)>
                    aggregateInputList.add(timeDifferenceFromStart + "," + Math.round(timeDifferenceFromStart/1000) + "," + Math.round(count * 1000.0 / timeDifferenceFromStart) + "," + Math.round(Constants.STATUS_REPORTING_WINDOW_INPUT  * 1000.0/timeDifference) + "," + percentageCompleted);
                    prevTime = currentTime;
                }

                line = br.readLine();
                count++;
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (Throwable e) {
            e.printStackTrace();
        }

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
                                                                  Float.parseFloat(fare_amount), //These currency values can be coded to  two bytes 
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
                                //eventBufferList.push(eventData);
                                eventBufferList.put(eventData);
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
    				try {
	                    event = (Object[])eventBufferList.take();
                    } catch (InterruptedException e1) {
	                    // TODO Auto-generated catch block
	                    e1.printStackTrace();
                    }
    				
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
