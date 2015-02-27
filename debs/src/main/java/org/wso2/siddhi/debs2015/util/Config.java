/**
 * 
 */
package org.wso2.siddhi.debs2015.util;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @author Miyuru Dayarathna
 *
 */
public class Config {
	public static String getConfigurationInfo(String key){
		Properties props = new Properties();
		InputStream inStream = null;
		String value = null;
		
		try {
			inStream = new FileInputStream("debs2015.properties");
			props.load(inStream);
			value = props.getProperty(key);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (inStream != null) {
				try {
					inStream.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		
		return value;
	}
}
