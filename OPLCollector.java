/**
 * 
 */
package com.ibm.optim.sample.oplinterface;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.StructType;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

/**
 * Represents an OPL data model in Spark.
 * An OPL application data model (ADM) consists of a set of tables (Tuplesets), each with its own schema.
 * An ADM is represented by a map in which the keys are the table names and the values are the table schemas.
 * A builder is provided to create the ADM.
 * 
 * The OPLCollector holds the actual data in Spark Datasets. There are several ways to populate
 * the data. 
 * Spark SQL operations can transform tables into other tables. 
 * A builder is provided when the data is generated programmatically.
 * JSON deserialization and serialization are provided when data is exchanged with external applications or stores.
 * Handling JSON through custom generator and parser methods provides more flexibility than
 * default Spark allows: JSON files can have formats more general than one line = one object, and
 * JSON sources other than files (e.g. streams) can be used.
 * 
 * The design of the OPLCollector class aims to reduce the amount of data that must be 
 * manipulated outside of Spark. Where possible, data is streamed among applications without
 * creating auxiliary in-memory structures or files.
 * 
 * The design of OPLCollector and its companion OPLTuple also aim to minimize the amount 
 * of custom coding required to build an application. Collectors and tuples are configured 
 * by specifying their schemas through builders rather than by extending with subclasses.
 * 
 * @author bloomj
 *
 */
public class OPLCollector {

	private String name;
	private Map<String, Dataset<Row>> sparkData;
	private Map<String, StructType> applicationDataModel;
	private Map<String, Long> size;
	private JsonDestination jsonDestination;
	private JsonSource jsonSource;
	
/*Constructors***********************************************************/
	
	/**
	 * Constructs a new OPLCollector instance.
	 * @param collectorName
	 * @param applicationDataModel the application data model (set to empty if not present)
	 * @param sparkData the application data as a set of Spark data sets (set to empty if not present)
	 */
	public OPLCollector(String collectorName) {
		super();
		this.name= collectorName;
		this.sparkData= new LinkedHashMap<String, Dataset<Row>>();
		this.applicationDataModel= new LinkedHashMap<String, StructType>();
		this.size= new LinkedHashMap<String, Long>();
		this.jsonDestination= null;
		this.jsonSource= null;
	}
	
	public OPLCollector(String collectorName, Map<String, StructType> applicationDataModel) {
		this(collectorName);
		this.applicationDataModel= applicationDataModel;
		for(String tableName: applicationDataModel.keySet())
			this.size.put(tableName, null);
	}
	
	public OPLCollector(String collectorName, Map<String, StructType> applicationDataModel, Map<String, Dataset<Row>> sparkData) {
		this(collectorName, applicationDataModel);
		this.sparkData= sparkData;
	}
	
	/**
	 * Creates a new OPLCollector instance with copies of the application data model and Spark datasets of this collector.
	 * The ADM copy is immutable. The Spark datasets themselves are immutable, but the copy supports the addTable, addData, and replaceTable methods.  
	 * Does not copy the JSONSource or JSONDestination fields.
	 * 
	 * @param name of the new collector 
	 * @param tableNames tables to be copied (all tables in this collector, if absent)
	 * @return a new OPLCollector instance
	 */
	public OPLCollector copy(String name) {
		OPLCollector result= new OPLCollector(name, this.getADM(), new LinkedHashMap<String, Dataset<Row>>(this.sparkData));
		result.size=  new LinkedHashMap<String, Long>(this.size); 
		return result;
	}
	
	public OPLCollector copy(String name, List<String> tableNames) {
		OPLCollector result= new OPLCollector(name);
		OPLCollector.ADMBuilder admBuilder= result.buildADM();
		for(String table: tableNames)
			admBuilder.addSchema(table, this.getSchema(table));
		admBuilder.build();
		OPLCollector.DataBuilder dataBuilder= result.buildData();
		for(String table: tableNames) {
			dataBuilder.addTable(table, this.getTable(table));
			result.size.put(table, this.getSize(table));
		}
		dataBuilder.build();
		return result;
	}
	
	public OPLCollector copy(String name, String... tableNames) {
		return copy(name, Arrays.asList(tableNames));
	}
	
/*Utility Methods********************************************************/
	
	/**
	 * Returns the name of this collector.
	 * @return collector name
	 */
	public String getName() {
		return name;
	}

	/**
	 * Adds a set of tables of data from another collector.
	 * An individual table can be set only once.
	 * 
	 * @param other
	 * @return this collector
	 * @throws IllegalArgumentException - if the other collector is empty or if any table in the other collector is already present
	 */
	public OPLCollector addTables(OPLCollector other) {
		if(other.applicationDataModel.isEmpty())
			throw new IllegalArgumentException("empty collector");
		for(String tableName: other.applicationDataModel.keySet())
			if(this.applicationDataModel.containsKey(tableName))
				throw new IllegalArgumentException("table " + tableName + " has already been defined");				
		this.applicationDataModel.putAll(other.applicationDataModel);
		this.sparkData.putAll(other.sparkData);
		this.size.putAll(other.size);
		return this;
	}
	
	/**
	 * Replaces an individual table of data.
	 * 
	 * @param tableName
	 * @param table
	 * @param length number of rows in table (null if omitted)
	 * @throws IllegalArgumentException - if the table has not been defined
	 */
	public void replaceTable(String tableName, Dataset<Row> table, Long size) {
		if(!this.sparkData.containsKey(tableName))
			throw new IllegalArgumentException("table " + tableName + "has not been defined");				
		this.sparkData.put(tableName, table);
		table.createOrReplaceTempView(tableName);
		this.size.put(tableName, size);
	}
	
	public void replaceTable(String tableName, Dataset<Row> table) {
		replaceTable(tableName, table, null);
	}
	
	/**
	 * Adds data to an existing table.
	 * Use when a table has several input sources.
	 * Does not deduplicate the data (i.e. allows duplicate rows).
	 * 
	 * @param tableName the existing table
	 * @param table the data to be added (must have the same schema as the existing table)
	 * @param length of the data set to be added
	 * @return this collector
	 */
	public OPLCollector addData(String tableName, Dataset<Row> table, Long size) {
		if(!this.sparkData.containsKey(tableName))
			throw new IllegalArgumentException("table " + tableName + "has not been defined");				
		this.sparkData.put(tableName, this.getTable(tableName).union(table));
		this.sparkData.get(tableName).createOrReplaceTempView(tableName);
		Long sum= (this.size.get(tableName)!= null && size!=null) ? this.size.get(tableName)+size : null;
		this.size.put(tableName, sum);
		return this;
	}
	
	/**
	 * Returns the table names
	 * @return
	 */
	public Set<String> getTableNames() {
		return Collections.unmodifiableSet(this.applicationDataModel.keySet());
	}
	
	/**
	 * Exposes the application data model for this OPLCollector.
	 * The ADM is represented by a map in which the keys are the table names 
	 * and the values are the table schemas held in Spark StructType objects.
	 * 
	 * @return an unmodifiable view of the application data model
	 */
	public Map<String, StructType> getADM() {
		return Collections.unmodifiableMap(this.applicationDataModel);
	}
	
	/**
	 * Sets the application data model for this OPLCollector.
	 * The ADM cannot be changed once set.
	 * 
	 * @param applicationDataModel
	 * @return this OPLCollector
	 * @throws IllegalArgumentException - the ADM has already been set
	 */
	public OPLCollector setADM(Map<String, StructType> applicationDataModel) {
		if (!this.applicationDataModel.isEmpty())
			throw new IllegalArgumentException("Application Data Model has already been set");
		this.applicationDataModel= applicationDataModel;
		for(String tableName: this.getTableNames())
			this.size.put(tableName, null);
		return this;
	}
	
	/**
	 * Creates a builder for the application data model for this collector.
	 * 
	 * @return a new ADMBuilder instance
	 */
	public OPLCollector.ADMBuilder buildADM() {
		return new OPLCollector.ADMBuilder(this);
	}
	
	/**
	 * Exposes the data tables in this OPLCollector.
	 * 
	 * @return an unmodifiable view of the data
	 */
	public Map<String, Dataset<Row>> getData() {
		return Collections.unmodifiableMap(sparkData);
	}
	
	/**
	 * Creates a builder for the data tables for this collector.
	 * Uses this collector's application data model.
	 * 
	 * @return a new DataBuilder instance
	 * @throws IllegalStateException if the data tables have already been loaded into this collector
	 */
	public OPLCollector.DataBuilder buildData() {
		if (!this.sparkData.isEmpty())
			throw new IllegalStateException("data tables have already been loaded");		
		return new OPLCollector.DataBuilder(this);	
	}
	
	/**
	 * Returns an individual table of data.
	 * 
	 * @param tableName
	 * @return a Spark dataframe
	 */
	public Dataset<Row> getTable(String tableName) {
		return sparkData.get(tableName);
	}
	
	/**
	 * Gets a list of all the values of a column in a table.
	 * 
	 * @param tableName
	 * @param columnName
	 * @return
	 */
	public List<String> getStringColumn(String tableName, String columnName) {
		List<String> result= new LinkedList<String>();
		List<Row> rows= this.getTable(tableName).select(columnName).collectAsList();
		for(Row row: rows)
			result.add(row.getString(row.fieldIndex(columnName)));		
		return result;
	}
	
	public List<Integer> getIntegerColumn(String tableName, String columnName) {
		List<Integer> result= new LinkedList<Integer>();
		List<Row> rows= this.getTable(tableName).select(columnName).collectAsList();
		for(Row row: rows)
			result.add(row.getInt(row.fieldIndex(columnName)));		
		return result;
	}
	
	public List<Double> getFloatColumn(String tableName, String columnName) {
		List<Double> result= new LinkedList<Double>();
		List<Row> rows= this.getTable(tableName).select(columnName).collectAsList();
		for(Row row: rows)
			result.add(row.getDouble(row.fieldIndex(columnName)));		
		return result;
	}
	
	public List<List<Integer>> getIntArrayColumn(String tableName, String columnName) {
		List<List<Integer>> result= new LinkedList<List<Integer>>();
		List<Row> rows= this.getTable(tableName).select(columnName).collectAsList();
		OPLTuple tuple;
		for(Row row: rows) {
			tuple= new OPLTuple(OPLTuple.defaultName(tableName), applicationDataModel.get(tableName), row);
			result.add(tuple.getIntArrayField(columnName));	
		}
		return result;
	}
	
	public List<List<Double>> getFloatArrayColumn(String tableName, String columnName) {
		List<List<Double>> result= new LinkedList<List<Double>>();
		List<Row> rows= this.getTable(tableName).select(columnName).collectAsList();
		OPLTuple tuple;
		for(Row row: rows) {
			tuple= new OPLTuple(OPLTuple.defaultName(tableName), applicationDataModel.get(tableName), row);
			result.add(tuple.getFloatArrayField(columnName));	
		}
		return result;
	}
	
	/**
	 * Returns the schema of an individual table of data.
	 * 
	 * @param tableName
	 * @return a Spark StructType
	 */
	public StructType getSchema(String tableName) {
		if(sparkData.isEmpty())
			throw new IllegalStateException("data tables have not been loaded");
		return sparkData.get(tableName).schema();
	}
	
	/**
	 * Returns a subset of the application data model.
	 * 
	 * @param tableNames table schemas to include
	 * @return a subset of the application data model
	 */
	public Map<String, StructType> selectSchemas(String... tableNames) {
		Map<String, StructType> result= new LinkedHashMap<String, StructType>();
		for(String tableName: tableNames)
			result.put(tableName, this.getSchema(tableName));
		return result;
	}
	
	/**
	 * Creates a new OPLCollector from a subset of the tables in this collector.
	 * The tables in the new collector are copies of the tables in the original.
	 * 
	 * @param collectorName of the new instance
	 * @param tableNames tables to include
	 * @return a new collector instance
	 */
	public OPLCollector selectTables(String collectorName, String... tableNames) {
		OPLCollector result;
		Map<String, StructType> adm= this.selectSchemas(tableNames);
		Map<String, Dataset<Row>> tables= new LinkedHashMap<String, Dataset<Row>>();
		RDD<Row> table;
		for(String tableName: tableNames) {
			table= this.getTable(tableName).rdd();
			tables.put(tableName, OPLGlobal.SPARK_SESSION.createDataFrame(table, this.getSchema(tableName)));
		}
		result= new OPLCollector(collectorName, adm, tables);
		for(String tableName: tableNames)
			result.size.put(tableName, this.getSize(tableName));
		return result;
	}
	
	/**
	 * Returns the number of rows in a table.
	 * Note: the Spark dataset count method is fairly expensive, 
	 * so it is used only if there is no other way to count the number of rows.
	 * It is best to count the rows as the table is being deserialized, as is done in the fromJSON method. 
	 * Once counted, the number is stored in the length map for future use.
	 * 
	 * @param tableName
	 * @return the number of rows in the table
	 */
	public Long getSize(String tableName) {
		if(!this.size.containsKey(tableName))
			throw new IllegalArgumentException("length not defined for table "+ tableName);
		if(this.size.get(tableName)==null)
			this.size.put(tableName, new Long(this.sparkData.get(tableName).count()));
		return this.size.get(tableName);
	}
	
	/**
	 * Creates a Spark encoder for a dataset with the specified schema.
	 * Encoders are used for various dataset operations, e.g. map.
	 * 
	 * @param schema
	 * @return the encoder
	 */
	public static Encoder<Row> encoder(StructType schema) {
		return RowEncoder.apply(schema);
	}
	
	/**
	 * Creates a Spark encoder for the dataset with the given name in this collector.
	 * Encoders are used for various dataset operations, e.g. map.
	 * 
	 * @param tableName
	 * @return the encoder
	 */
	public Encoder<Row> getEncoder(String tableName) {
		return encoder(applicationDataModel.get(tableName));
	}
	
	/**
	 * Prints the contents of a table.
	 * A bare-bones implementation with minimal formating.
	 * 
	 * @param tableName
	 * @param out the destination stream
	 */
	public void display(String tableName, PrintStream out) {
		out.println("Table: " + tableName);
		Dataset<Row> table= this.getTable(tableName);
		Iterator<Row> rows= table.toLocalIterator();
		while(rows.hasNext())
			out.println(rows.next());
		out.println();		
	}
	
	/**
	 * Prints the contents of all tables in this collector.
	 * A bare-bones implementation with minimal formating.
	 * 
	 * @param out the destination stream
	 */
	public void display(PrintStream out) {
		out.println(this.getName());
		for(String tableName: this.getTableNames())
			display(tableName, out);
	}
	
	/**
	 * Prints the contents of a table.
	 * Uses Spark dataset show format.
	 * 
	 * @param tableName name to be displayed
	 * @param table
	 * @param numRows  - Number of rows to show. Default is the size of the table.
	 * @param out the destination stream
	 */
	public static void show(String tableName, Dataset<Row> table, Long numRows, PrintStream out) {
		out.println("table: " + tableName);
		PrintStream saved= System.out;
		System.setOut(out);
		table.show(numRows.intValue());
		System.setOut(saved);			
	}
	
	public static void show(String tableName, Dataset<Row> table, PrintStream out) {
		show(tableName, table, table.count(), out);
	}
	
	/**
	 * Prints the contents of a table.
	 * Uses Spark dataset show format.
	 * 
	 * @param tableName
	 * @param numRows  - Number of rows to show. Default is the size of the table.
	 * @param out the destination stream
	 */
	public void show(String tableName, Long numRows, PrintStream out) {
		out.println("collector: " + this.getName());
		out.println("table: " + tableName);	
		PrintStream saved= System.out;
		System.setOut(out);
		this.getTable(tableName).show(numRows.intValue());
		System.setOut(saved);			
			
	}
	
	public void show(String tableName, PrintStream out) {
		this.show(tableName, this.getSize(tableName), out);		
	}
	
	/**
	 * Prints the contents of all tables in this collector.
	 * Uses Spark dataset show format.
	 * 
	 * @param numRows  - Number of rows of each table to show. Default is size of the table.
	 * @param out the destination stream
	 */
	public void show(Long numRows, PrintStream out) {
		for(String tableName: this.getTableNames()) {
			if (numRows>0)
				this.show(tableName, numRows, out);	
			else
				this.show(tableName, out);
		}
	}
	
	public void show(PrintStream out) {
		this.show(0L, out);
	}
	
	/**
	 * Displays the schema of a table.
	 * 
	 * @param tableName
	 * @param out the destination stream
	 */
	public void showSchema(String tableName, PrintStream out) {
		out.println("table: " + tableName);
		PrintStream saved= System.out;
		System.setOut(out);
		this.getTable(tableName).printSchema();
		System.setOut(saved);				
	}
	
	/**
	 * Displays application data model, the schemas of all the tables.
	 * 
	 * @param tableName
	 * @param out the destination stream
	 */
	public void showADM(PrintStream out) {
		out.println("collector: " + this.getName());
		for(String tableName: this.getData().keySet()) {
			showSchema(tableName, out);
		}
		
	}
	
	
/*Application Data Model*************************************************/
	
	/**
	 * Builds an Application Data Model that associates a set of Spark Datasets with their schemas.
	 * Usage:
	 * 
	 * 	Map<String, StructType> adm= (new OPLCollector.ADMBuilder())
	 * 		.addSchema("warehouse", (new OPLTuple.SchemaBuilder()
	 * 			.addField("location", DataTypes.StringType)
	 * 			.addField("capacity", DataTypes.DoubleType)
	 * 			.buildSchema())
	 * 		.addSchema("route", (new OPLTuple.SchemaBuilder()
	 * 			.addField("from", DataTypes.StringType)
	 * 			.addField("to", DataTypes.StringType)
	 * 			.addField("capacity", DataTypes.DoubleType)
	 * 			.buildSchema())
	 * 		.build();
	 * 
	 * @author bloomj
	 *
	 */
	public static class ADMBuilder {

		private OPLCollector collector;
		private Map<String, StructType> result;
		
		/**
		 * Creates a new builder.
		 * @param collector if present, loads the application data model directly into the collector;
		 * if not present or null, the ADM map is returned directly
		 */
		public ADMBuilder(OPLCollector collector) {
			super();
			this.collector= collector;
			this.result= new LinkedHashMap<String, StructType>();
		}
		
		public ADMBuilder() {
			this(null);
		}
		
		/**
		 * Adds a new table schema to the ADM.
		 * 
		 * @param tableName
		 * @param tupleSchema can be built with OPLTuple.SchemaBuilder
		 * @return this builder
		 */
		public ADMBuilder addSchema(String tableName, StructType tupleSchema) {
			if(result.containsKey(tableName))
				throw new IllegalArgumentException("tuple schema " + tableName + "has already been defined");
			result.put(tableName, tupleSchema);
			return this;
		}
		
		/**
		 * Enables referring to a schema in the ADM under construction to create a new schema.
		 * 
		 * @param tableName
		 * @return the schema
		 */
		public StructType referenceSchema(String tableName) {
			if(!result.containsKey(tableName))
				throw new IllegalArgumentException(tableName + " does not exist");			
			return result.get(tableName);
		}
		
		/**
		 * Completes building the application data model.
		 * If an OPLCollector has been supplied in the constructor, loads the ADM into it.
		 * 
		 * @return the ADM
		 * @throws IllegalStateException if the ADM for the collector has already been defined
		 */
		public Map<String, StructType> build() {
			if(collector!=null) {
				if (!collector.applicationDataModel.isEmpty())
					throw new IllegalStateException("application data model has already been defined");
				collector.applicationDataModel= result;			
			}
			return result;
		}
		
	}/*class OPLCollector.ADMBuilder*/
	
/*Spark Data*************************************************************/
	
	/**
	 * Builds the Spark datasets to hold the application data.
	 * Used when the data are created programmatically.
	 * 
	 * @author bloomj
	 *
	 */
	public static class DataBuilder {
		
		private Map<String, StructType> adm;
		private OPLCollector collector;
		private Map<String, Dataset<Row>> result;
		private Map<String, Long> length;
		
		/**
		 * Creates a builder for loading the Spark datasets.
		 * 
		 * @param adm the application data model for the tables
		 * @param collector if present, loads the data tables and their sizes directly into the collector;
		 * if not present or null, the Spark data map is returned directly
		 * @throws UnsupportedOperationException if the application Data Model has not been defined
		 */
		public DataBuilder(Map<String, StructType> adm) {
			super();
			if(adm==null || adm.isEmpty())
				new UnsupportedOperationException("Application Data Model is not defined");
			this.adm= adm;
			this.collector= null;
			this.result= new LinkedHashMap<String, Dataset<Row>>();	
			this.length= new LinkedHashMap<String, Long>();
		}
		
		public DataBuilder(OPLCollector collector) {
			this(collector.applicationDataModel);
			this.collector= collector;
		}
		
		/**
		 * Gets the external data and creates the corresponding application dataset to add to the collector.
		 * Registers the data set as a Spark SQL table.
		 * Assumes that the schema of this table is already present in the ADM.
		 * 
		 * @param tableName the name of the table
		 * @param data
		 * @param length number of rows in table (null if omitted)
		 * @return this builder instance
		 * @throws IllegalArgumentException - if the table is not included in the ADM or if the table has already been loaded
		 */
		public DataBuilder addTable(String tableName, Dataset<Row> data, Long size) {
			if(!adm.containsKey(tableName))
				throw new IllegalArgumentException(tableName + " is not an ADM table");
			if(result.containsKey(tableName))
				throw new IllegalArgumentException(tableName + " is already defined");			
			this.result.put(tableName, data);
			data.createOrReplaceTempView(tableName);
			this.length.put(tableName, size);
			return this;		
		}

		public DataBuilder addTable(String tableName, Dataset<Row> data) {
			return addTable(tableName, data, null);
		}
		
		public DataBuilder addTable(String tableName, List<Row> data) {
			return addTable(tableName, OPLGlobal.SPARK_SESSION.createDataFrame(data, adm.get(tableName)), (long) data.size());
		}
		
		/**
		 * Adds an empty table to the collector.
		 * Used to create a placeholder for later adding data to or replacing the table.
		 * 
		 * @param tableName the name of the table
		 * @return this builder instance
		 * @throws IllegalArgumentException - if the table is not included in the ADM or if the table has already been loaded
		 */
		public DataBuilder addEmptyTable(String tableName) {
			return addTable(tableName, new ArrayList<Row>(1));
		}
		
		/**
		 * Enables referring to a table in the collector under construction to create a new table.
		 * Can be used in SQL statements. 
		 * 
		 * @param tableName
		 * @return the table
		 */
		public Dataset<Row> referenceTable(String tableName) {
			if(!result.containsKey(tableName))
				throw new IllegalArgumentException(tableName + " does not exist");
			return result.get(tableName);
		}
		
		/**
		 * Completes building the Spark data.
		 * If an OPLCollector has been supplied in the constructor, loads the data tables and their sizes into it.
		 * 
		 * @return a map of table names to Spark data sets containing the application data
		 * @throws IllegalArgumentException - if a table in the ADM has no associated data
		 * @throws IllegalStateException if data tables have already been loaded into the collector
		 */
		public Map<String, Dataset<Row>> build() {
			for(String tableName: adm.keySet())
				if(!result.containsKey(tableName))
					throw new IllegalStateException(tableName + " has no data");
			if(collector!=null) {
				if (!collector.sparkData.isEmpty())
					throw new IllegalStateException("data tables have already been loaded");
				collector.sparkData= this.result;
				collector.size= this.length;
			}
			return result;
		}
		
		/**
		 * @return an unmodifiable view of the size map created by this builder
		 * Note: calling this method before the build method could return an inaccurate result
		 */
		public Map<String, Long> retrieveSize() {
			return Collections.unmodifiableMap(this.length);
		}
				
	}/*class OPLCollector.DataBuilder*/
	
/*Serialization**********************************************************/
	
	/**
	 * Provides a means to write the application data as JSON.
	 * You must first set the destination (an output stream, file, url, or string) where the JSON will be written.
	 * Then you call the serializer toJSON method.
	 * 
	 * Usage:
	 *  dataCollector.setJsonDestination(new File("data.json").toJSON();
	 *  
	 * @return this collector
	 *
	 * @author bloomj
	 */
	public OPLCollector toJSON() {
		
		JsonGenerator generator= this.getJsonDestination().getGenerator();
		
		try {
			generator.writeStartObject();
			for(String tableName: this.getTableNames()) {
				this.serialize(tableName);
			}
			generator.writeEndObject();		
			generator.close();
			this.getJsonDestination().getStream().flush();
			this.getJsonDestination().getStream().close();
		} catch (IOException e) {
			System.err.println("Unable to generate JSON " + e.getMessage());
			e.printStackTrace();
		}
		return this;
		
	}/*toJSON*/

	/**
	 * Provides a means to write an individual data table as JSON.
	 *
	 * @author bloomj
	 */
	void serialize(String tableName) throws IOException {
		
		JsonGenerator generator= this.getJsonDestination().getGenerator();
		Dataset<Row> table= this.sparkData.get(tableName);
		generator.writeArrayFieldStart(tableName);

//		the following causes: java.io.NotSerializableException: java.io.PrintStream
//		PrintStream out= new PrintStream(this.getJsonDestination().getStream(), true);
//		table.toJSON().foreach(row -> out.println(row));
//		Use this instead:		
		Iterator<String> rows= table.toJSON().toLocalIterator();
		while(rows.hasNext()) {
			generator.writeRaw(rows.next());
			if(rows.hasNext())
				generator.writeRaw(", \n");
		}	
		generator.writeEndArray();
	}/*serialize*/

	/**
	 * Sets the destination for the JSON serialization.
	 * Replaces an existing destination if one has been set previously.
	 * Use saveDestination to save the current destination (preserving the current state of the JSON generator), 
	 * if needed, before setting a new destination.
	 * 
	 * @param destination an output stream, file, or URL 
	 * (note: if the serialization should go to a string, use a ByteArrayOuputStream as the destination and use its toString method to get the string.)
	 * @return this collector instance
	 * @throws class dependent exceptions generated by errors in opening the destination
	 */
	public OPLCollector setJsonDestination(OutputStream destination) {
		this.jsonDestination= new JsonDestination(destination);
		return this;
	}

	public OPLCollector setJsonDestination(File destination) {
		JsonDestination result= null;	
		try {
			result= new JsonDestination(new FileOutputStream(destination));
		} catch (FileNotFoundException e) {
			System.err.println("Failed to find destination file " + e.getMessage());
			e.printStackTrace();
		}
		this.jsonDestination= result;
		return this;
	}

	public OPLCollector setJsonDestination(URL destination) {
		JsonDestination result= null;	
		try {
			URLConnection connection= destination.openConnection();
			connection.setDoOutput(true);	
			result= new JsonDestination(connection.getOutputStream());
		} catch (IOException e) {
			System.err.println("Failed to open URL connection " + e.getMessage());
			e.printStackTrace();
		}
		this.jsonDestination= result;
		return this;
	}
	
	JsonDestination getJsonDestination() {
		if(jsonDestination==null)
			throw new UnsupportedOperationException("JSON destination has not been set");
		return jsonDestination;
	}
	
	public OutputStream getDestinationStream() {
		return this.getJsonDestination().getStream();
	}

	public JsonDestination saveDestination() {
		if(jsonDestination==null)
			throw new UnsupportedOperationException("JSON destination has not been set");
		return new JsonDestination(this.jsonDestination);		
	}
	
	public OPLCollector restoreDestination(JsonDestination saved) {
		this.jsonDestination= saved;
		return this;
	}

	/**
	 * Represents an output stream, file, or URL destination for the JSON serialization of an OPLCollector.
	 * Normally, a JsonDestination object is created by on of the public setJsonDestination methods.
	 * You should not need to work with a JsonDestination object directly.
	 * 
	 * @author bloomj
	 *
	 */
	static class JsonDestination {
		
		private JsonGenerator generator;
		private OutputStream stream;
		
		private JsonDestination(OutputStream destination) {
			super();
			this.stream= destination;
			try {
				this.generator= OPLGlobal.JSON_FACTORY.createGenerator(destination, JsonEncoding.UTF8);
				this.generator.enable(JsonGenerator.Feature.AUTO_CLOSE_TARGET);
				this.generator.useDefaultPrettyPrinter();
			} catch (IOException e) {
				System.err.println("Failed to create JSON generator " + e.getMessage());
				e.printStackTrace();
			}
		}
		
		private JsonDestination(JsonDestination other) {
			super();
			this.stream= other.stream;
			this.generator= other.generator;
		}
		
		JsonGenerator getGenerator() {
			return generator;
		}
		
		OutputStream getStream() {
			return stream;
		}	
		
	}/*class OPLCollector.JsonDestination*/
	
/*Deserialization********************************************************/
	
	/**
	 * Provides a means to create a collector from JSON.
	 * You must first set the source (an output stream, file, url, or string) where the JSON will be read.
	 * Then you call the deserializer fromJSON method.
	 * The collector's application data model corresponding to the JSON input file must already have been created.
	 * 
	 * Optionally, you can apply a transformation to a table's tuples as they are read from the JSON file.
	 * When a transformation has been applied to a table, its output schema replaces the table's input schema
	 * in the collector's application data model.
	 * Transformations are useful when they are simple, and the Spark map operation might be difficult to apply because
	 * the transformation cannot be serialized. However, since it operates in parallel, using map may be more efficient 
	 * for complex transformations. 
	 * 
	 * There is a one-to-one correspondence between an OPLCollector instance and its JSON representation;
	 * that is, the JSON source file must fully include all the data tables to be populated in the collector instance.
	 * Methods are provided to merge two collectors with separate JSON sources (addTables), 
	 * add a data set to a collector (addTable), and to add data from a data set to an existing table in a collector.
	 * 
	 * The deserialization implemented in OPLCollector is more general than is supported by Spark, in two ways.
	 * First, it supports a more general JSON format than the one table per file and one object per line format allowed in Spark.
	 * Second, it supports other data sources than files (e.g. streams) and supports data streaming without creating intermediate
	 * in-memory or file data stores (note: in this context, streaming is used in the traditional sense, 
	 * not the real-time sense of the Spark Streaming facility).
	 * 
	 * The deserialization is implemented using the Jackson Streaming API, which avoids creating additional in-memory data structures.
	 * 
	 * Usage:
	 * 	OPLCollector results= (new OPLCollector("results", resultsDataModel)).setJsonSource(new File("results.json")).fromJSON(transformations);
     *  
	 * @param transformations to the tables' tuples as they are read, one per table and present only if a transformation is to be applied to a table (empty if none are present)
	 * @return this collector
	 */
	public OPLCollector fromJSON(OPLTuple.List.Deserializer.Transformer... transformations) {
		Map<String, OPLTuple.List.Deserializer.Transformer> tableTransformations= new LinkedHashMap<String, OPLTuple.List.Deserializer.Transformer>();
		for(OPLTuple.List.Deserializer.Transformer t: transformations) {
			if(!this.applicationDataModel.containsKey(t.getTableName()))
				throw new IllegalArgumentException(t.getTableName() + " is not an ADM table");
			if(tableTransformations.containsKey(t.getTableName()))
				throw new IllegalArgumentException("transformation already specified for table " + t.getTableName());
			tableTransformations.put(t.getTableName(), t);
		}
			
		OPLCollector.Deserializer deserializer= new OPLCollector.Deserializer(this, tableTransformations);	
		deserializer.initialize();
		if(deserializer.isEmpty())
			throw new IllegalArgumentException("JSON collector object is empty");
		
		OPLCollector.DataBuilder dataBuilder= this.buildData();
		Dataset<Row> table;
		String tableName;
		while(deserializer.hasNext()) {
			table= deserializer.next();
			tableName= deserializer.getCurrentTableName();
			dataBuilder= dataBuilder.addTable(tableName, table, deserializer.getCurrentTableSize());
			if(tableTransformations.containsKey(tableName))
				this.applicationDataModel.put(tableName, tableTransformations.get(tableName).getOutputSchema());				
		}
		dataBuilder.build();
		return this;
	}/*fromJSON*/
	
	public OPLCollector fromJSON() {
		return this.fromJSON(new OPLTuple.List.Deserializer.Transformer[0]);
	}
	
	/**
	 * Sets the source for the JSON text that populates the collector.
	 * There is a one-to-one correspondence between an OPLCollector instance and its JSON representation;
	 * that is, the JSON source file must fully include all the data tables to be populated in the collector instance. 
	 * Thus, it makes no sense to have more than one JSON source for a collector or to change JSON sources.
	 * 
	 * @param source the input stream, file, URL or string that provides the JSON text
	 * @return this collector
	 * @throws IllegalArgumentException is the JSON source for this collector has already been defined.
	 * @throws class dependent exceptions generated by errors in opening the source
	 */
	public OPLCollector setJsonSource(InputStream source) {
		if(this.jsonSource!=null)
			throw new IllegalArgumentException("JSON source has already been set");
		this.jsonSource= new JsonSource(source);
		return this;
	}

	public OPLCollector setJsonSource(File source) {
		if(this.jsonSource!=null)
			throw new IllegalArgumentException("JSON source has already been set");
		InputStream stream= null;
		try {
			stream= new FileInputStream(source);
		} catch (FileNotFoundException e) {
			System.err.println("Failed to find source file " + e.getMessage());
			e.printStackTrace();
		}
		return setJsonSource(stream);		
	}
	
	public OPLCollector setJsonSource(URL source) {
		if(this.jsonSource!=null)
			throw new IllegalArgumentException("JSON source has already been set");
		InputStream stream= null;
		URLConnection connection;		
		try {
			connection= source.openConnection();
			connection.setDoInput(true);	
			stream= connection.getInputStream();
		} catch (IOException e) {
			System.err.println("Failed to open URL connection " + e.getMessage());
			e.printStackTrace();
		}		
		return setJsonSource(stream);		
	}
	
	public OPLCollector setJsonSource(String source) {
		if(this.jsonSource!=null)
			throw new IllegalArgumentException("JSON source has already been set");
		InputStream stream= null;
		stream=  new ByteArrayInputStream(source.getBytes());
		return setJsonSource(stream);		
	}
	
	JsonSource getJsonSource() {
		if(this.jsonSource==null)
			throw new UnsupportedOperationException("JSON source has not been set");
		return this.jsonSource;
	}
	
	public InputStream getJsonSourceStream() {
		return this.getJsonSource().getStream();
	}

	static class JsonSource {
		
		private JsonParser parser;
		private InputStream stream;
		
		private JsonSource(InputStream stream) {
			super();
			this.stream= stream;
			try {
				this.parser= OPLGlobal.JSON_FACTORY.createParser(stream);
				this.parser.enable(JsonParser.Feature.AUTO_CLOSE_SOURCE);
			} catch (IOException e) {
				System.err.println("Failed to create JSON parser " + e.getMessage());
				e.printStackTrace();
			}			
		}

		JsonParser getParser() {
			return parser;
		}	
		
		InputStream getStream() {
			return stream;
		}

	}/*class OPLCollector.JsonSource*/
	
	/**
	 * Provides a means to create a collector from JSON.
	 * The deserializer is structured as a Java iterator over the collector's tables (i.e. Spark datasets).
	 * @see fromJSON 
	 * 
	 * @author bloomj
	 *
	 */
	public static class Deserializer implements java.util.Iterator<Dataset<Row>> {
		
		private OPLCollector collector;
		private String tableName;
		private Long tableSize;
		private Map<String, OPLTuple.List.Deserializer.Transformer> tableTransformations;
		private JsonParser parser;
		private boolean isInitialized;
		private boolean isEmpty;
	
		/**
		 * Creates a new Deserializer for a collector.
		 * 
		 * @param collector the collector to be deserialized. It must already have an application data model and must not have tables already loaded.
		 * @param tableTransformations optional transformations to be applied to the tables' tuples as they are read from the JSON file (empty if not present)
		 */
		public Deserializer(OPLCollector collector, Map<String, OPLTuple.List.Deserializer.Transformer> tableTransformations) {
			super();
			this.collector= collector;
			this.tableTransformations= tableTransformations;
			this.tableName= null;
			this.tableSize= null;
			this.parser= this.collector.getJsonSource().getParser();
			this.isInitialized= false;
			this.isEmpty= true;
		}
		
		public Deserializer(OPLCollector collector) {
			this(collector, new LinkedHashMap<String, OPLTuple.List.Deserializer.Transformer>());
		}
		
		/**
		 * @return the name of the table currently being read
		 */
		public String getCurrentTableName() {
			return this.tableName;
		}
		
		/**
		 * @return the size of the table that just has been read (note, returns null while the table is being read)
		 */
		public Long getCurrentTableSize() {
			return this.tableSize;
		}
		
		/**
		 * Initializes the deserializer and positions the JSON parser at the start of the first table data array.
		 * Note, initialize must be called before calling isEmpty, hasNext, or next; otherwise they will throw an exception.
		 * 
		 * @throws IOException is the parser has not been positioned at the start of the table object where its name can be read
		 */
		public void initialize() {
			try {
				parser.nextToken();
//				System.out.println("In OPLCollector.deserialize 0: next token= " + parser.getCurrentToken().toString());
				if (parser.getCurrentToken() != JsonToken.START_OBJECT) {
					throw new IOException("Expected to start an object");
				}
				parser.nextValue(); //Table name and array start
				this.isEmpty= this.parser.getCurrentToken()==JsonToken.END_OBJECT;
				this.isInitialized= true;
			} catch (IOException e) {
				System.err.println("JSON parsing error: " + e.getMessage());
				e.printStackTrace();
			}
		}
		
		/**
		 * Checks whether the collector generated by this Deserializer has any tables in it.
		 * 
		 * @return true is the collector contains no tables, true otherwise
		 * @throws IllegalStateException if the deserializer has not been initialized
		 */
		public boolean isEmpty() {
			if(!this.isInitialized)
				throw new IllegalStateException("Deserializer must be intialized");
			return this.isEmpty;
		}
		
		/**
		 * @return true unless the parser has reached the end of the collector object
		 * @throws IllegalStateException if the deserializer has not been initialized
		 * @see java.util.Iterator#hasNext()
		 */
		@Override
		public boolean hasNext() {
			if(!isInitialized)
				throw new IllegalStateException("Deserializer must be intialized");
			if(this.isEmpty())
				return false;
			return parser.getCurrentToken() != JsonToken.END_OBJECT; /*not at end of collector data*/
		}

		/**
		 * Reads the next table object from the JSON file.
		 * First, it gets the table name, which must correspond to an entry in the application data model.
		 * Then it creates and initializes a deserializer for the table's tuples.
		 * Then it uses the tupleset deserializer to read the table's JSON array and creates a corresponding Spark dataset.
		 * It applies a transformation to the tables tuples as they are read, if any has been specified.
		 * Then it captures the size of the table that was read.
		 * Finally, it positions the parser at the start of the next table.
		 * If it has read the last table, it closes the parser and its associated input stream.
		 * 
		 * @throws IllegalStateException if the deserializer has not been initialized
		 * @throws NoSuchElementException if the parser has read the last table in the file
		 * @throws IllegalArgumentException if the name of the table has no entry in the collector's application data model
		 * @throws IOException if the parser encounters an error
		 * 
		 * @see java.util.Iterator#next()
		 */
		@Override
		public Dataset<Row> next() {
			if(!hasNext())
				throw new NoSuchElementException();
//			System.out.println("In OPLCollector.deserialize 1: next token= " + parser.getCurrentToken().toString());
			OPLTuple.List tupleset= null;
			Dataset<Row> table= null;
			this.tableSize= null;
			try {
				this.tableName = parser.getCurrentName();
//				System.out.println("In OPLCollector.deserialize 2: table name= " + tableName);
				if(!collector.applicationDataModel.containsKey(this.tableName))
					throw new IllegalArgumentException("table " + this.tableName + " is not defined in the application data model");
				tupleset= OPLTuple.List.fromJSON(this.tableName, collector.applicationDataModel.get(tableName), this.tableTransformations.get(tableName), parser);
				table= tupleset.toDataset();
				this.tableSize= Long.valueOf(tupleset.size());
				parser.nextValue(); //Next table name and array start
				
				//finally
				if(!hasNext()) {
					parser.close();
					collector.getJsonSource().getStream().close();				
				}
			} catch (IOException e) {
				System.err.println("JSON parsing error: " + e.getMessage());
				e.printStackTrace();
			}	
			return table;
		}
		
	}/*class OPLCollector.Deserializer*/
	

}/*class OPLCollector*/
