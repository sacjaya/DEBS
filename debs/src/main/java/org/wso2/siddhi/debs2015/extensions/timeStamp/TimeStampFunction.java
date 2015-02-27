package org.wso2.siddhi.debs2015.extensions.timeStamp;

import org.apache.log4j.Logger;
import org.wso2.siddhi.core.config.SiddhiContext;
import org.wso2.siddhi.core.executor.function.FunctionExecutor;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.extension.annotation.SiddhiExtension;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Created by sachini on 1/19/15.
 */
@SiddhiExtension(namespace = "debs", function = "getTimestamp")
public class TimeStampFunction extends FunctionExecutor {
	private static final Logger logger = Logger.getLogger(TimeStampFunction.class);
    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    //2013-01-02 08:36:00
    @Override
    public void init(Attribute.Type[] attributeTypes, SiddhiContext siddhiContext) {

    }

    @Override
    protected Object process(Object data) {
    	//+Miyuru : We can directly cast the object to String while parsing
//        String dateAndTime = (String) data;
//
        Date d = null;
        
        try {
//            d = sdf.parse(dateAndTime);
            
            d = sdf.parse((String)data);
        } catch (ParseException e) {
        	logger.error(e.getMessage());
        }

//+Miyuru : It would be better to get the time value directly from the Date object
//        Calendar c = Calendar.getInstance();
//        c.setTime(d);
//        return c.getTimeInMillis();
        
        return d.getTime();
    }

    public void destroy() {

    }

    public Attribute.Type getReturnType() {
        return Attribute.Type.LONG;
    }

}
