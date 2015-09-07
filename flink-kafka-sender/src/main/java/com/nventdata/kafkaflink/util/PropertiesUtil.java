package com.nventdata.kafkaflink.util;

import java.io.IOException;
import java.util.Properties;

public class PropertiesUtil {

	public static Properties getPropertiesFromClassPath(String propName){
		Properties prop = new Properties();
		try {
			prop.load(PropertiesUtil.class.getClassLoader().getResourceAsStream(propName));
		} catch (IOException e) {
			e.printStackTrace();
		}
		return prop;
	}
}
