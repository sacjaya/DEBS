package org.wso2.siddhi.debs2015.extensions.cellId;

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
    private Logger log = Logger.getLogger(CellIdFunction.class);
    private float  westMostLongitude = -74.916578f; //previous -74.916586f;
    private float  eastMostLongitude = -73.120778f; //previous -73.116f;
    private float  longitudeDifference =eastMostLongitude-westMostLongitude ;
    private float  northMostLatitude = 41.477182778f; //previous 41.477185f;
    private float  southMostLatitude = 40.129715978f; //previous 40.128f;
    private float  latitudeDifference = northMostLatitude-southMostLatitude ;
    private short  gridResolution = 300; //This is the number of cells per side in the square of the simulated area.

    @Override
    public void init(Attribute.Type[] attributeTypes, SiddhiContext siddhiContext) {

    }

    @Override
    protected Object process(Object data) {
    	//--------------------------------------------------          The following is for longitude -------------------------------------
    	
        float inputLongitude = Float.parseFloat(String.valueOf(((Object[])data)[0]));
        //+Miyuru: better to declare the variable as short so that we can save memory
        //int cellIdFirstComponent;
        short cellIdFirstComponent;
        
        if(westMostLongitude==inputLongitude){
            cellIdFirstComponent= gridResolution;
        } else {
            cellIdFirstComponent = (short) ((((eastMostLongitude - inputLongitude) / longitudeDifference) * gridResolution) + 1);
        }
        
        //+Miyuru: Why don't we simply return null if the cell is out of the permitted range?
        //that way we can avoid creating additional String literals "-".
        if(cellIdFirstComponent<0 ||cellIdFirstComponent>gridResolution){
            return "-";
        }

        
        //--------------------------------------------------          The following is for latitude -------------------------------------
        float inputLatitude = Float.parseFloat(String.valueOf(((Object[])data)[1]));
        
        //int cellIdSecondComponent;
        short cellIdSecondComponent;
        
        if(southMostLatitude==inputLatitude){
            cellIdSecondComponent= gridResolution;
        } else {
            cellIdSecondComponent = (short) ((((northMostLatitude - inputLatitude) / latitudeDifference) * gridResolution) + 1);
        }
        
        if(cellIdSecondComponent<0 ||cellIdSecondComponent>gridResolution){
            return "-";
        }
        
        return cellIdFirstComponent+"."+cellIdSecondComponent;
    }

    public void destroy() {

    }

    public Attribute.Type getReturnType() {
        return Attribute.Type.STRING;
    }
}
