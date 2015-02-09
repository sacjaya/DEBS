package extensions.timeStamp;

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
    static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    //2013-01-02 08:36:00
    @Override
    public void init(Attribute.Type[] attributeTypes, SiddhiContext siddhiContext) {

    }

    @Override
    protected Object process(Object data) {
        String dateAndTime = (String) data;

        Date d = null;
        try {
            d = sdf.parse(dateAndTime);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        Calendar c = Calendar.getInstance();
        c.setTime(d);
        return c.getTimeInMillis();
    }

    @Override
    public void destroy() {

    }

    @Override
    public Attribute.Type getReturnType() {
        return Attribute.Type.LONG;
    }

}
