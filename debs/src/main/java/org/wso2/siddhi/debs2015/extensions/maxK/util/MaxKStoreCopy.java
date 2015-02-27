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

package org.wso2.siddhi.debs2015.extensions.maxK.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

public class MaxKStoreCopy {
    //Holds the Max K readings
    private Map<String, Long> units = new ConcurrentHashMap<String, Long>();
    private Map<Long, List<String>> maxValues  = new TreeMap<Long, List<String>>();
    //No of data to be held in the Map: The value of K

    /**
     * Calculated the current top k values by comparing the values that are
     * already stored in the Map.
     *
     * @return A Map that contains the Max-K values
     * @params value - The pressure reading value for the current event
     * @params date - The timestamp the pressure reading was produced.
     *
     */
    public synchronized Map<Long, List<String>> getMaxK(String cell, Long count) {
        if(count==0){
        	//We have to make sure that the units contains the cell id we are looking for. Otherwise we may get a NullPointerException.
        	if (units.containsKey(cell)) {
                Long previousCount = units.get(cell);
                units.remove(cell);
                maxValues.get(previousCount).remove(cell);
        	}
        } else {
        	//This code basically updates the count per cell. If there is a new value for the count
        	//which is non-zero, the old value is replaced with the new value.
        	
        	//The maxValues TreeMap holds the list of cells for each count.
        	//"units" is an index of which the key is the cell ID and the value is the count.
        	
            if (units.containsKey(cell)) {
                Long previousCount = units.get(cell);
                units.put(cell, count);
                maxValues.get(previousCount).remove(cell);
                List<String> cellsList = maxValues.get(count);
                if (cellsList != null) {
                    cellsList.add(cell);
                } else {
                    cellsList = new ArrayList<String>();
                    cellsList.add(cell);
                    maxValues.put(count, cellsList);
                }
            } else {
                units.put(cell, count);
                List<String> cellsList = maxValues.get(count);
                if (cellsList != null) {
                    cellsList.add(cell);
                } else {
                    cellsList = new ArrayList<String>();
                    cellsList.add(cell);
                    maxValues.put(count, cellsList);
                }
            }
        }

        // Returns the pressure readings that are sorted in descending order according to the key (pressure value).
        return new TreeMap<Long, List<String>>(maxValues).descendingMap();

    }
}
