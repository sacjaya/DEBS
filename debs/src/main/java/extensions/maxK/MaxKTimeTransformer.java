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
package extensions.maxK;

import com.clearspring.analytics.stream.Counter;
import com.clearspring.analytics.stream.StreamSummary;
import extensions.maxK.util.MaxKStoreCopy;
import org.apache.log4j.Logger;
import org.wso2.siddhi.core.config.SiddhiContext;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.event.in.InEvent;
import org.wso2.siddhi.core.event.in.InListEvent;
import org.wso2.siddhi.core.event.in.InStream;
import org.wso2.siddhi.core.exception.QueryCreationException;
import org.wso2.siddhi.core.executor.expression.ExpressionExecutor;
import org.wso2.siddhi.core.query.processor.transform.TransformProcessor;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.StreamDefinition;
import org.wso2.siddhi.query.api.expression.Expression;
import org.wso2.siddhi.query.api.expression.Variable;
import org.wso2.siddhi.query.api.expression.constant.IntConstant;
import org.wso2.siddhi.query.api.extension.annotation.SiddhiExtension;

import java.util.List;
import java.util.Map;


@SiddhiExtension(namespace = "MaxK", function = "getMaxK")
public class MaxKTimeTransformer extends TransformProcessor {

   private static final Logger LOGGER = Logger.getLogger(MaxKTimeTransformer.class);
    private boolean debugEnabled = false;

    private String value = "";
    private String startCell = "";
    private String endCell = "";
    //The desired attribute position of value in input stream
    private int valuePosition = 0;
    private int startCellPosition = 0;
    private int endCellPosition = 0;
    //The K value
    private int kValue = 0;
    //An array of Objects to manipulate output stream elements
    private Object[] data = null;
    private Object[] previousData = null;
    private boolean duplicate =true;

    private MaxKStoreCopy maxKStore = null;


    StreamSummary<String> topk = new StreamSummary<String>();
    List<Counter<String>> counters;

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

            this.value = (String) objects[0];
            this.valuePosition = (Integer) objects[1];
            this.startCell = (String) objects[2];
            this.startCellPosition = (Integer) objects[3];
            this.endCell = (String) objects[4];
            this.endCellPosition = (Integer) objects[5];
            this.kValue = (Integer) objects[6];
            this.maxKStore = (MaxKStoreCopy) objects[7];

        } else {
            LOGGER.error("Failed in restoring the Max-K Transformer.");
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

        if (expressions.length != 4) {
            LOGGER.error("Required Parameters : Four");
            throw new QueryCreationException("Mismatching Parameter count.");
        }

        //Getting all the parameters and assign those to instance variables
        value = ((Variable) expressions[0]).getAttributeName();
        startCell = ((Variable) expressions[1]).getAttributeName();
        endCell = ((Variable) expressions[2]).getAttributeName();
        kValue = ((IntConstant) expressions[3]).getValue();

        valuePosition = inStreamDefinition.getAttributePosition(value);
        startCellPosition = inStreamDefinition.getAttributePosition(startCell);
        endCellPosition = inStreamDefinition.getAttributePosition(endCell);

        this.outStreamDefinition = new StreamDefinition().name("MaxKStream");
        for (int i = 1; i <= kValue; i++) {
            this.outStreamDefinition.attribute("startCell" + i , Attribute.Type.STRING);
            this.outStreamDefinition.attribute("endCell" + i , Attribute.Type.STRING);
        }

        for(Attribute attribute:inStreamDefinition.getAttributeList()){
            this.outStreamDefinition.attribute(attribute.getName(),attribute.getType());
        }
        this.outStreamDefinition.attribute("duplicate", Attribute.Type.BOOL);
        //Initiate the data object array that is to be sent with output stream
        data = new Object[2 * kValue+inStreamDefinition.getAttributeList().size()+1];
        previousData = new Object[2 * kValue];
        maxKStore = new MaxKStoreCopy();

        //If the reset time is grater than zero, then starting the ScheduledExecutorService instance that will schedule resetting stream-lib connector.

        long count = 0;


    }

    @Override
    public void destroy() {

    }

    private void processEventForMaxK(InEvent event) {
        Object eventKeyValue = event.getData(valuePosition);
        Object startCellValue = event.getData(startCellPosition);
        Object endCellValue = event.getData(endCellPosition);

        topk.offer(startCellValue+":"+endCellValue);
//        topk.peek()
//
//        Object eventKeyValue = event.getData(valuePosition);
//        Object startCellValue = event.getData(startCellPosition);
//        Object endCellValue = event.getData(endCellPosition);
//
//
//        Map<Integer, List<String>> currentTopK;
//
//        currentTopK = maxKStore.getMaxK(startCellValue+":"+endCellValue, (Integer) eventKeyValue);
//
//        int position = 0;
//
//        for(List<String> cellList: currentTopK.values()){
//            for (String cell:cellList){
//                String[] splitValues = cell.split(":");
//                data[position++] = splitValues[0];
//                data[position++] = splitValues[1];
//                if(position>=kValue*2)
//                    break;
//            }
//            if(position>=kValue*2)
//                break;
//        }
//
//        //Populating remaing elements for the payload of the stream.
//        while (position < (2 * kValue)) {
//            data[position++] = "null";
//            data[position++] = "null";
//        }
//
//        for(int i=0;i<position;i++){
//            if(previousData[i]== null || !previousData[i].equals(data[i])) {
//                  duplicate = false;
//            }
//            previousData[i] = data[i];
//        }
//
//        for (Object value:event.getData()) {
//            data[position++] = value;
//        }
//
//        data[position++] = duplicate;
//        duplicate = true;
//
//        if (debugEnabled) {
//            LOGGER.debug("Latest Top-K elements with frequency" + data);
//        }

    }

}
