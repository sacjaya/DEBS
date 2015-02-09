package extensions.median;

import com.google.common.collect.MinMaxPriorityQueue;
import org.wso2.siddhi.core.query.selector.attribute.handler.OutputAttributeAggregator;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by sachini on 1/9/15.
 */
public class MedianAggregator_1 implements OutputAttributeAggregator {
    final MinMaxPriorityQueue<Float> minHeap = MinMaxPriorityQueue.<Float>create();
    final MinMaxPriorityQueue<Float> maxHeap = MinMaxPriorityQueue.<Float>create();
    int totalElements = 0;

    @Override
    public Attribute.Type getReturnType() {
        return Attribute.Type.FLOAT;
    }

    @Override
    public Object processAdd(Object obj) {
        Float  element = (Float) obj;
        if (totalElements % 2 == 0) {
            maxHeap.add(element);
            totalElements++;

            if (minHeap.isEmpty()) {
                return getMedian();
            }
            if (maxHeap.peekFirst() < minHeap.peekLast()) {
                float minValue = maxHeap.removeFirst();
                float maxValue = minHeap.removeLast();
                minHeap.add(minValue);
                maxHeap.add(maxValue);
            }
        } else {
            maxHeap.add(element);
            float minValue = maxHeap.removeFirst();
            minHeap.add(minValue);
            totalElements++;

        }

        return getMedian();
    }

    @Override
    public Object processRemove(Object obj) {
        Float  element = (Float) obj;

        if (totalElements % 2 == 0) {
            totalElements--;

            if (maxHeap.peekFirst() >= element) {
                maxHeap.remove();
                float value = minHeap.removeLast();
                maxHeap.add(value);
            } else {
                if (!minHeap.isEmpty()) {
                    minHeap.remove(element);
                }
            }
        } else {
            totalElements--;
            if (maxHeap.peekFirst() >= element) {
                maxHeap.remove();
            } else {
                float value = maxHeap.removeFirst();
                minHeap.add(value);
            }

        }


        return getMedian();
    }

    @Override
    public OutputAttributeAggregator newInstance() {
        return new MedianAggregator_1();
    }

    @Override
    public void destroy() {

    }

    public float getMedian() {

        if (totalElements % 2 == 0) {
            return (maxHeap.peekFirst() + minHeap.peekLast()) / 2;
        } else {
            return maxHeap.peekFirst();
        }
    }
}
