/**
 * 
 */
package org.wso2.siddhi.debs2015.util;

/**
 * @author Miyuru Dayarathna
 *
 * This class consists of the general set of constants referred throughout the DEBS 2015 project 
 */
public class Constants {
	public static int EVENT_COUNT_PARTIAL_DATASET=1999999; //This is for the small data set given by the DEBS 2015 organizers.
	public static int EVENT_COUNT_FULL_DATASET=173185091;  //This is for the complete data set.
	public static int STATUS_REPORTING_WINDOW_INPUT=10000;	   //The number of events which needs to be passed to the query network before the status gets reported.
	public static int STATUS_REPORTING_WINDOW_OUTPUT=100;
	public static int EVENT_BUFFER_FLOOR=360000;			//When this many events are left in the input events buffer, the data from the file needs to be read and loaded to the buffer until it reaches EVENT_BUFFER_CEIL 
	public static int EVENT_BUFFER_CEIL=EVENT_BUFFER_FLOOR * 2; //This many events in the events buffer is required to start sending the events to query network.
	public static int EVENT_LIST_SIZE_POLLING_WINDOW=100000;
	public static final int MONITORING_THREAD_SLEEP_TIME = 1000;
}