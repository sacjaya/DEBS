/*
*  Copyright (c) 2005-2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
*  WSO2 Inc. licenses this file to you under the Apache License,
*  Version 2.0 (the "License"); you may not use this file except
*  in compliance with the License.
*  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.wso2.siddhi.debs2015.extensions.maxK;

import org.apache.log4j.Logger;
import org.wso2.siddhi.core.config.SiddhiContext;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.event.in.InEvent;
import org.wso2.siddhi.core.event.in.InListEvent;
import org.wso2.siddhi.core.event.in.InStream;
import org.wso2.siddhi.core.exception.QueryCreationException;
import org.wso2.siddhi.core.executor.expression.ExpressionExecutor;
import org.wso2.siddhi.core.query.processor.transform.TransformProcessor;
import org.wso2.siddhi.debs2015.extensions.maxK.util.MaxKStoreCopy;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.StreamDefinition;
import org.wso2.siddhi.query.api.expression.Expression;
import org.wso2.siddhi.query.api.expression.Variable;
import org.wso2.siddhi.query.api.expression.constant.IntConstant;
import org.wso2.siddhi.query.api.expression.constant.LongConstant;
import org.wso2.siddhi.query.api.extension.annotation.SiddhiExtension;

import java.util.List;
import java.util.Map;


@SiddhiExtension(namespace = "MaxK", function = "getMaxK")
public class CopyOfMaxKTimeTransformerCopy2 extends TransformProcessor {

   private static final Logger LOGGER = Logger.getLogger(CopyOfMaxKTimeTransformerCopy2.class);
    private boolean debugEnabled = false;

    private String value = "";
    private String startCell = "";
    private String endCell = "";
    private String iijTimeStamp = "";
    //The desired attribute position of value in input stream
    private int valuePosition = 0;
    private int startCellPosition = 0;
    private int endCellPosition = 0;
    private int iijTimeStampPosition = 0;
    //The K value
    private int kValue = 0;
    //private long iijTimeStamp = 0;
    //An array of Objects to manipulate output stream elements
    private Object[] data = null;
    private Object[] previousData = null;
    private boolean duplicate =true;
//    private boolean eventLengthFlag=true;
    
    private int pickUpDateTimeFieldID=2;
    private int dropOffDataTimeFieldID=3;
    private int timeStampFieldID=5;
    
    private MaxKStoreCopy maxKStore = null;

    @Override
    protected InStream processEvent(InEvent inEvent) {
        if (debugEnabled) {
            LOGGER.debug("Processing a new Event for TopK Determination, Event : " + inEvent);
        }
        processEventForMaxK(inEvent);
        return new InEvent(inEvent.getStreamId(), System.currentTimeMillis(), data);
    }

    @Override
    protected InStream processEvent(InListEvent inListEvent) {
        InListEvent transformedListEvent = new InListEvent();
        for (Event event : inListEvent.getEvents()) {
            if (event instanceof InEvent) {
                transformedListEvent.addEvent((Event) processEvent((InEvent) event));
            }
        }
        return transformedListEvent;
    }

    @Override
    protected Object[] currentState() {
        return new Object[]{value,valuePosition,startCell,startCellPosition,endCell,endCellPosition, kValue,maxKStore};
    }

    @Override
    protected void restoreState(Object[] objects) {
        if ((objects.length == 8) &&
                (objects[0] instanceof String) && (objects[1] instanceof Integer) &&
                (objects[2] instanceof String) && (objects[3] instanceof Integer) &&
                (objects[4] instanceof String) && (objects[5] instanceof Integer) &&
                (objects[6] instanceof Integer) &&
                (objects[7] instanceof MaxKStoreCopy) ) {
        	//tripCount, startCellNo, endCellNo, 10, iij_timestamp
            this.value = (String) objects[0]; //value corresponds to trip count
            this.valuePosition = (Integer) objects[1];
            
            this.startCell = (String) objects[2];
            this.startCellPosition = (Integer) objects[3];
            
            this.endCell = (String) objects[4];
            this.endCellPosition = (Integer) objects[5];
            
            this.kValue = (Integer) objects[6];
            this.maxKStore = (MaxKStoreCopy) objects[7];

        } else {
            //LOGGER.error("Failed in restoring the Max-K Transformer.");
        	System.out.println("Failed in restoring the Max-K Transformer.");
        }
    }

    @Override
    protected void init(Expression[] expressions,
                        List<ExpressionExecutor> expressionExecutors,
                        StreamDefinition inStreamDefinition,
                        StreamDefinition outStreamDefinition,
                        String elementId,
                        SiddhiContext siddhiContext) {

        debugEnabled = LOGGER.isDebugEnabled();

        if (expressions.length != 5) {
            LOGGER.error("Required Parameters : Five");
            throw new QueryCreationException("Mismatching Parameter count.");
        }

        //Getting all the parameters and assign those to instance variables
        //tripCount, startCellNo, endCellNo, 10, iij_timestamp
        value = ((Variable) expressions[0]).getAttributeName();
        startCell = ((Variable) expressions[1]).getAttributeName();
        endCell = ((Variable) expressions[2]).getAttributeName();
        kValue = ((IntConstant) expressions[3]).getValue();
              
        iijTimeStamp = ((Variable) expressions[4]).getAttributeName();
        
//        System.out.println("iijTimeStamp : " + iijTimeStamp);
//        System.exit(0);
        
        valuePosition = inStreamDefinition.getAttributePosition(value);
        startCellPosition = inStreamDefinition.getAttributePosition(startCell);
        endCellPosition = inStreamDefinition.getAttributePosition(endCell);
        iijTimeStampPosition = inStreamDefinition.getAttributePosition(iijTimeStamp);

        this.outStreamDefinition = new StreamDefinition().name("MaxKStream");
        for (int i = 1; i <= kValue; i++) {
            this.outStreamDefinition.attribute("startCell" + i , Attribute.Type.STRING);
            this.outStreamDefinition.attribute("endCell" + i , Attribute.Type.STRING);
        }

//        //By adding the input attributes to the output stream we are copying the events we received from the previous operator to down stream.
//        for(Attribute attribute:inStreamDefinition.getAttributeList()){
//            this.outStreamDefinition.attribute(attribute.getName(),attribute.getType());
//        }
        this.outStreamDefinition.attribute("pickup_datetime", Attribute.Type.STRING);
        this.outStreamDefinition.attribute("dropoff_datetime", Attribute.Type.STRING);
        this.outStreamDefinition.attribute("iij_timestamp", Attribute.Type.LONG);
        
        //Finally, we add a flag that indicates whether this is a duplicate event or not.
        this.outStreamDefinition.attribute("duplicate", Attribute.Type.BOOL);
        
        //Initiate the data object array that is to be sent with output stream
        //inStreamDefinition.getAttributeList() returns 6
        //data = new Object[2 * kValue+inStreamDefinition.getAttributeList().size()+1]; //This will be (2*10 + (6+1))=27
        data = new Object[2 * kValue+4]; //This will be (2*10 + 4)=24
        previousData = new Object[2 * kValue];
        maxKStore = new MaxKStoreCopy();

        //If the reset time is grater than zero, then starting the ScheduledExecutorService instance that will schedule resetting stream-lib connector.


    }

//    @Override
//    public void destroy() {
//
//    }

    private void processEventForMaxK(InEvent event) {

        Object eventKeyValue = event.getData(valuePosition);//This is the trip count
        Object startCellValue = event.getData(startCellPosition);
        Object endCellValue = event.getData(endCellPosition);
        
//        if(eventLengthFlag){
//        	eventLengthFlag = false;
//        	timeStampFieldID = event.getData().length - 1;
//        	pickUpDateTimeFieldID = timeStampFieldID - 
//        }

        Map<Long, List<String>> currentTopK;

        //The method getMaxK() accepts the "<start cell ID>:<end cell ID>" and the trip count found for this route.
        currentTopK = maxKStore.getMaxK(startCellValue+":"+endCellValue, (Long) eventKeyValue);

        //From here onwards we prepare the output data tuple from this operator.
        int position = 0;

        for(List<String> cellList: currentTopK.values()){
        	//We do this until top-k is 10 (kValue==10)
            for (String cell:cellList){
                String[] splitValues = cell.split(":");
                data[position++] = splitValues[0];//start cell ID
                data[position++] = splitValues[1];//end cell ID
                if(position>=kValue*2){
                    break;
                }
            }
            
            if(position>=kValue*2){
                break;
            }
        }

        //Populating remaining elements for the payload of the stream with null if we could not find the top-k number of routes.
        while (position < (2 * kValue)) {
            data[position++] = "null";
            data[position++] = "null";
        }

        for(int i=0;i<position;i++){
        	//If the previous data we recorded was null or we have different data recorded in this tuple, we set the duplicate flag to false.
        	//Note that by default, the duplicate flag is set to true.
        	//But in most of the cases where previous data item is set to null or its updated, the duplicate flag is set to false indicating that this is not a duplicate output.
            if(previousData[i]== null || !previousData[i].equals(data[i])) {
                  duplicate = false;
            }
            previousData[i] = data[i];
        }

//        //In this for loop we are copying the input data tuple which triggered this event to the output stream.
//        for (Object value:event.getData()) {
//            data[position++] = value;
//        }
        data[position++] = event.getData()[pickUpDateTimeFieldID];
        data[position++] = event.getData()[dropOffDataTimeFieldID];
        data[position++] = event.getData()[timeStampFieldID];
        
        //And finally we set the duplicate flag.
        data[position++] = duplicate;
        duplicate = true;	//we reset the Duplicate flag here.

        if (debugEnabled) {
            LOGGER.debug("Latest Top-K elements with frequency" + data);
        }

    }

	public void destroy() {
    }

}
