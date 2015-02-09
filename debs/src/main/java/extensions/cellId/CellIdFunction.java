package extensions.cellId;

import org.apache.log4j.Logger;
import org.wso2.siddhi.core.config.SiddhiContext;
import org.wso2.siddhi.core.executor.function.FunctionExecutor;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.extension.annotation.SiddhiExtension;

/**
* Created by sachini on 1/7/15.
*/
@SiddhiExtension(namespace = "debs", function = "cellId")
public class CellIdFunction extends FunctionExecutor {
    Logger log = Logger.getLogger(CellIdFunction.class);
    private float  leftMostLongitude = -74.916586f;
    private float  rightMostLongitude = -73.116f;
    private float  longitudeDifference =rightMostLongitude-leftMostLongitude ;
    private float  northMostLatitude =  41.477185f;
    private float  southMostLatitude = 40.128f;
    private float  latitudeDifference = northMostLatitude-southMostLatitude ;

    @Override
    public void init(Attribute.Type[] attributeTypes, SiddhiContext siddhiContext) {

    }

    @Override
    protected Object process(Object data) {
        float inputLongitude = Float.parseFloat(String.valueOf(((Object[])data)[0]));
        int cellIdFirstComponent;
        if(leftMostLongitude==inputLongitude){
            cellIdFirstComponent= 300;
        } else {
            cellIdFirstComponent = (int) ((((rightMostLongitude - inputLongitude) / longitudeDifference) * 300) + 1);
        }
        if(cellIdFirstComponent<0 ||cellIdFirstComponent>300){
            return "-";
        }

        float inputLatitude = Float.parseFloat(String.valueOf(((Object[])data)[1]));
        int cellIdSecondComponent;
        if(southMostLatitude==inputLatitude){
            cellIdSecondComponent= 300;
        } else {
            cellIdSecondComponent = (int) ((((northMostLatitude - inputLatitude) / latitudeDifference) * 300) + 1);
        }
        if(cellIdSecondComponent<0 ||cellIdSecondComponent>300){
            return "-";
        }
        return cellIdFirstComponent+"."+cellIdSecondComponent;


    }

    @Override
    public void destroy() {

    }

    @Override
    public Attribute.Type getReturnType() {
        return Attribute.Type.STRING;
    }
}
