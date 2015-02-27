package org.wso2.siddhi.debs2015.extensions.median;

import org.wso2.siddhi.core.query.selector.attribute.handler.OutputAttributeAggregator;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by sachini on 1/9/15.
 */
public class MedianAggregator implements OutputAttributeAggregator {
    List<Float> values  = new ArrayList<Float>();
    public Attribute.Type getReturnType() {
        return Attribute.Type.FLOAT;
    }

    public Object processAdd(Object obj) {
        values.add((Float) obj);
        return getMedian();
    }

    public Object processRemove(Object obj) {
        values.remove((Float)obj);
        return getMedian();
    }

    public OutputAttributeAggregator newInstance() {
        return new MedianAggregator();
    }

    public void destroy() {

    }

    private float getMedian(){
        Collections.sort(values);
        int size = values.size();
        if(size==0){
            return 0;
        }

        if(size==1){
            return values.get(0);
        } else if(size%2==1){
            return values.get(size/2);
        } else {
            return (values.get((size/2)-1)+values.get(size/2))/2;
        }

    }
}
