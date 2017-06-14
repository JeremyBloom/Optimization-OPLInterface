/**
 * 
 */
package com.ibm.optim.sample.oplinterface;

import java.util.Random;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;

import com.fasterxml.jackson.core.JsonFactory;
import com.ibm.optim.oaas.sample.display.DisplayWindow;
import com.ibm.optim.oaas.sample.display.DisplayWindow.ExtendedPrintStream;

/**
 * Provides access to global objects supporting the application.
 * 
 * @author bloomj
 *
 */
public class OPLGlobal {

	public static final SparkConf SPARK_CONFIGURATION = new SparkConf().setAppName("OPL").setMaster("local[*]");
//	public static final SparkContext SPARK_CONTEXT = new SparkContext(SPARK_CONFIGURATION);
	public static final SparkSession SPARK_SESSION= SparkSession.builder()
			.config(SPARK_CONFIGURATION)
			.config("spark.sql.warehouse.dir", "file:///C:/Users/IBM_ADMIN/Documents/SparkSQLWarehouse/")
			.getOrCreate();
	
	public static final JsonFactory JSON_FACTORY= new JsonFactory();
	
	private static DisplayWindow window= new DisplayWindow("");
	public static ExtendedPrintStream out= window.printStream();
	
	public static Random randomGenerator= new Random(17);

	public static void setDisplayTitle(String title) {
		window.setTitle(title);
	}
	
	public static void showDisplay() {
		window.show();
	}
	

}/*class OPLGlobal*/
