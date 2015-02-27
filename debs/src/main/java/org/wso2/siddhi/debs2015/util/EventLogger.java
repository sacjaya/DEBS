/**
 * 
 */
package org.wso2.siddhi.debs2015.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.LinkedList;

import org.wso2.siddhi.core.event.Event;

/**
 * This is a utility class to log the event stream to a file
 * @author Miyuru Dayarathna
 *
 */
public class EventLogger extends Thread {
	FileWriter fw = null;
	BufferedWriter bw = null;
	private static LinkedList<Event[]> eventsList = new LinkedList<Event[]>();
	
	public EventLogger(String fileName){
		try {
            fw = new FileWriter(new File(fileName).getAbsoluteFile());
			bw = new BufferedWriter(fw);
        } catch (IOException e1) {
            e1.printStackTrace();
        }
	}
	
	public void run(){
		Event[] eventArray = null;
		
		while(true){
			eventArray = eventsList.poll();
			
			if (eventArray != null){
				try {
    				for(Event evt : eventArray){ 
    	                    bw.write(evt.toString());
    	                    bw.write("\r\n");
    				}
    				bw.flush();
                } catch (IOException e) {
                    e.printStackTrace();
                }
			}
			
			//We sleep for half a second to avoid the CPU core being over utilized.
			try {
                Thread.currentThread().sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
		}
	}
	
	public void addEvents(Event[] events){
		eventsList.add(events);
	}
}
