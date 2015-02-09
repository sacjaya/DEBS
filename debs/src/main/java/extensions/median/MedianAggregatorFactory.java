package extensions.median;

import org.wso2.siddhi.core.query.selector.attribute.factory.OutputAttributeAggregatorFactory;
import org.wso2.siddhi.core.query.selector.attribute.handler.OutputAttributeAggregator;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.extension.annotation.SiddhiExtension;

/**
 * Created by sachini on 1/9/15.
 */
@SiddhiExtension(namespace = "custom", function = "median")
public class MedianAggregatorFactory implements OutputAttributeAggregatorFactory {
    @Override
    public OutputAttributeAggregator createAttributeAggregator(Attribute.Type[] types) {
        return new MedianAggregator();
    }
}
