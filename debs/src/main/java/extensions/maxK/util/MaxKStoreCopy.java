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

package extensions.maxK.util;

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

    /*
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
            Long previousCount = units.get(cell);
            units.remove(cell);
            maxValues.get(previousCount).remove(cell);
        } else {
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
