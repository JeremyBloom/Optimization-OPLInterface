/**
 * 
 */
package com.ibm.optim.sample.oplinterface;

import java.io.IOException;
import java.io.PrintStream;
import java.util.AbstractList;
import java.util.AbstractSequentialList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.ListIterator;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;

/**
 * Represents an OPL tuple. 
 * A tuple's data is held in a Spark row object and an OPL tupleset is represented by a Spark dataset of rows. 
 * Normally, a application developer need not work directly with OPLTuple objects since most operations will be implemented
 * by Spark dataset operations. The Spark datasets themselves are held by and accessed though OPLCollector objects. 
 * However, in creating the application data model of a collector, the developer will usually need to use the OPLTuple SchemaBuilder 
 * to specify the schema of each table.
 * 
 * In this design of the OPL interface, all tuples are represented by the same class and distinguished by their schemas.
 * Each OPLTuple has a schema defined in the constructor. The schema is immutable. 
 * 
 * The field data items are held privately, created by the constructor and accessed through getter methods. 
 * However, OPLTuple is not a bean-style class.
 *   
 * @author bloomj
 *
 */
public class OPLTuple {
	
	private String name;
	private StructType schema;
	private Row data;

	/**
	 * Constructs a new OPLTuple instance.
	 * 
	 * @param name of this tuple (all tuples with the same schema should have the same name)
	 * @param schema the schema of this tuple
	 * @param data the data elements of the tuple in the order specified by the schema (set to null if absent)
	 */
	public OPLTuple(String name, StructType schema) {
		super();
		this.name= name;
		this.schema= schema;
		this.data = null;
	}
	
	public OPLTuple(String name, StructType schema, Object... data) {
		this(name, schema);
		this.data = RowFactory.create(data);
	}
	
	public OPLTuple(String name, StructType schema, Collection<Object> data) {
		this(name, schema, data.toArray());
	}
	
	/**
	 * Constructs a new OPLTuple instance from a Spark Row.
	 * 
	 * @param name of this tuple (all tuples with the same schema should have the same name)
	 * @param schema of this tuple (if absent, the Row's schema is used, assuming it has been defined)
	 * @param data the data elements of this tuple in a Spark Row object
	 */
	public OPLTuple(String name, StructType schema, Row data) {
		this(name, schema);
		this.data= data;	
	}
	
	public OPLTuple(String name, Row data) {
		this(name, data.schema(), data);
	}
	
	public String getName() {
		return name;
	}
	
	/**
	 * Creates a default name: "T"+tableName without a trailing "s" if present.
	 * E.g. table name "warehouses" yields the default tuple name "TWarehouse"
	 * 
	 * @param tableName
	 * @return
	 */
	public static String defaultName(String tableName) {
		StringBuilder tupleName= new StringBuilder("T"+tableName);
		//Capitalize the table name
		tupleName.setCharAt(1, Character.toUpperCase(tupleName.charAt(1)));
		if(tupleName.charAt(tupleName.length()-1)=='s')
			tupleName.deleteCharAt(tupleName.length()-1);
		return tupleName.toString();
	}
	
	/**
	 * Sets (or replaces) the name of this tuple with a default "T"+tableName without a trailing "s" if present.
	 * E.g. table name "warehouses" yields the default tuple name "TWarehouse"
	 * 
	 * @param tableName
	 * @return
	 */
	public OPLTuple setDefaultName(String tableName) {
		this.name= defaultName(tableName).toString();
		return this;
	}

	/**
	 * Returns a new OPLTuple with a field's value replaced by a new value.
	 * 
	 * @param fieldName to be replaced
	 * @param value
	 * @return a new OPLTuple
	 */
	public OPLTuple replace(String fieldName, Object value) {
		OPLTuple.Builder builder= new OPLTuple.Builder(this.schema);
		builder.copy(this);
		builder.result.set(this.schema.fieldIndex(fieldName), value);
		return new OPLTuple(this.name, this.schema, builder.buildData());	
	}
	
	/**
	 * Returns a copy of the schema of this tuple. The schema itself is immutable.
	 * 
	 * @return a copy of the schema of this tuple
	 */
	public StructType schema() {
		return new StructType(this.schema.fields());
	}
	
	/**
	 * @return true if this tuple contains a field with the given name, false otherwise
	 */
	public boolean containsField(String fieldName) {
		return Arrays.asList(this.schema.fieldNames()).contains(fieldName);
	}
	
	/**
	 * Gets the data fields of this tuple as a Row.
	 * 
	 * @return the row
	 */
	public Row asRow() {
		return this.data;
	}
	
	/**
	 * @return the data fields of this tuple
	 */
	public Object[] asArray() {
		Object[] result= new Object[this.data.length()];
		for(int i=0; i<this.data.length(); i++)
			result[i]= this.data.get(i);
		return result;
	}
	
	/**
	 * @return a new Row instance with data fields equal to those of this tuple
	 */
	public Row toRow() {
		return RowFactory.create(this.asArray());		
	}
	
	/**
	 * @return a Spark dataset with a single row consisting of this tuple
	 */
	public Dataset<Row> asSingleton() {
		java.util.List<Row> row= new ArrayList<Row>(1);
		row.add(this.data);
		return OPLGlobal.SPARK_SESSION.createDataFrame(row, this.schema);
	}
	
	/**
	 * Gets the value of a string field in an OPL tuple.
	 * 
	 * @param fieldName the field whose value is returned
	 * @return the value of the field as a String
	 * @throws IllegalArgumentException - if the field is not defined
	 */
	public String getStringField(String fieldName) {
		return data.getString(schema.fieldIndex(fieldName));
	}
	
	/**
	 * Gets the value of an integer field in an OPL tuple.
	 * 
	 * @param fieldName the field whose value is set
	 * @return the value of the field as an Integer
	 * @throws IllegalArgumentException - if the field is not defined
	 */
	public Integer getIntField(String fieldName) {
		return data.getInt(schema.fieldIndex(fieldName));
	}
	
	/**
	 * Gets the value of an integer array field in an OPL tuple.
	 * 
	 * @param fieldName the field whose value is returned
	 * @return the value of the field as a List of Integer
	 * @throws IllegalArgumentException - if the field is not defined
	 */
	public java.util.List<Integer> getIntArrayField(String fieldName) {
		java.util.List<Object> list= data.getList(schema.fieldIndex(fieldName));
		return new AbstractList<Integer>(){

			@Override
			public Integer get(int index) {
				return (Integer)list.get(index);
			}

			@Override
			public int size() {
				return list.size();
			}
			
		}/*AbstractList*/;
	}
	
	/**
	 * Gets the value of a float field in an OPL tuple.
	 * Note float values in OPL correspond to Double values in Java.
	 * 
	 * @param fieldName the field whose value is returned
	 * @return the value of the field as a Double
	 * @throws IllegalArgumentException - if the field is not defined
	 */
	public Double getFloatField(String fieldName) {
		return data.getDouble(schema.fieldIndex(fieldName));
	}
	
	/**
	 * Gets the value of a float array field in an OPL tuple.
	 * Note float values in OPL correspond to Double values in Java.
	 * 
	 * @param fieldName the field whose value is returned
	 * @return the value of the field as a List of Double
	 * @throws IllegalArgumentException - if the field is not defined
	 */
	public java.util.List<Double> getFloatArrayField(String fieldName) {
		java.util.List<Object> list= data.getList(schema.fieldIndex(fieldName));
		return new AbstractList<Double>(){

			@Override
			public Double get(int index) {
				return (Double)list.get(index);
			}

			@Override
			public int size() {
				return list.size();
			}
			
		}/*AbstractList*/;
	}
	
	@Override
	public String toString() {
		return data.mkString(", ");
	}
	
	/**
	 * Builds a tuple schema.
	 * Strictly speaking, this builder is not needed since the StructType class provides the necessary functionality.
	 * However, it is provided as a convenience.
	 * Only the following data types are supported in the schema: String, integer, float (represented as Double), and 1-dimensional arrays of integer or float.
	 * The array types are supported only for internal use and cannot be serialized to or deserialized from JSON.
	 * 
	 * Usage:
	 * 	StructType warehouseSchema= (new OPLTuple.SchemaBuilder()).addField("location", DataTypes.StringType).addField("capacity", DataTypes.DoubleType).buildSchema();
	 * The fields of the resulting StructType are not nullable and have no metadata.
	 * 
	 * @author bloomj
	 *
	 */
	public static class SchemaBuilder {
		
		private Map<String, StructField> fields;

		public SchemaBuilder() {
			super();
			this.fields= new LinkedHashMap<String, StructField>();
		}
		
		public SchemaBuilder addField(StructField field) {
			if(this.fields.containsKey(field.name()))
				throw new IllegalArgumentException("field " + field.name() + " has already been set");
			this.fields.put(field.name(), field);			
			return this;
		}
		
		public SchemaBuilder addField(String name, DataType type) {
			this.addField(new StructField(name, type, false, Metadata.empty()));
			return this;
		}
		
		public SchemaBuilder copyField(StructType otherSchema, String fieldName) {
			this.addField(otherSchema.apply(fieldName));
			return this;
		}
				
		public SchemaBuilder copyFields(StructType otherSchema) {
			for(StructField field: otherSchema.fields()) {
				this.addField(field);
			}
			return this;
		}
		
		public StructType buildSchema() {
			return new StructType(fields.values().toArray(new StructField[0]));
		}
		
	}/*class OPLTuple.SchemaBuilder*/
	
	/**
	 * Creates a builder for the data for this tuple.
	 * Uses this tuple's schema.
	 * 
	 * Usage:
	 * 	OPLTuple warehouse= (new OPLTuple(warehouseSchema)).buildData().add("location", "NYC").add("capacity", 1000.0).build();
	 * 
	 * @return a new Builder instance
	 * @throws IllegalStateException if the data has already been loaded into this tuple
	 */
	public OPLTuple.Builder buildData() {
		if (this.data!=null && this.data.size()>0)
			throw new IllegalStateException("Tuple data has already been loaded");
		return new OPLTuple.Builder(this);
	}
	
	/**
	 * Builds the data for a tuple.
	 * @author bloomj
	 *
	 */
	public static class Builder {
		
		private StructType schema;		
		private HashSet<String> fields;
		private ArrayList<Object> result;
		private OPLTuple tuple;
		
		/**
		 * Creates a builder for loading Row data for an OPLTuple.
		 * 
		 * @param schema the schema of the data
		 * @param tuple if present, loads the data directly into the tuple;
		 * if not present or null, the Spark Row is returned directly
		 * @throws UnsupportedOperationException if the schema has not been defined
		 */
		public Builder(StructType schema) {
			super();
			if(schema==null)
				throw new IllegalArgumentException("Tuple schema has not been defined");
			this.schema= schema;
			this.tuple= null;
			this.fields= new HashSet<String>(schema.length());
			this.result= new ArrayList<Object>(Collections.nCopies(schema.length(), null));
		}
		
		public Builder(OPLTuple tuple) {
			this(tuple.schema);
			this.tuple= tuple;		
		}
		
		/**
		 * Adds a field to the data row.
		 * 
		 * @param fieldName
		 * @param value
		 * @return this builder
		 * @throws IllegalArgumentException if the field is not in the schema or if it has already been assigned a value 
		 */
		public Builder add(String fieldName, Object value) {
			if(!Arrays.asList(schema.fieldNames()).contains(fieldName))
					throw new IllegalArgumentException("field " + fieldName + " is not in the tuple schema");
			if(!this.fields.add(fieldName))
				throw new IllegalArgumentException("field " + fieldName + " has already been set");
			this.result.set(schema.fieldIndex(fieldName), value);
			return this;
		}
		
		/**
		 * Copies a data field from another tuple.
		 * 
		 * @param fieldName
		 * @param other the tuple to be copied
		 * @return this builder
		 */
		public Builder copy(String fieldName, OPLTuple other) {
			this.add(fieldName, other.data.get(other.schema.fieldIndex(fieldName)));
			return this;			
		}
		
		/**
		 * Copies the data from another tuple.
		 * 
		 * @param other the tuple to be copied
		 * @return this builder
		 */
		public Builder copy(OPLTuple other) {
			for(String fieldName: other.schema.fieldNames())
				this.copy(fieldName, other);
			return this;
		}
		
		/**
		 * Completes building the tuple data.
		 * If an OPLTuple has been supplied in the constructor, loads the data row into it.
		 * 
		 * @return the data as a Spark Row
		 * @throws IllegalStateException if a field in the schema has no data or if the tuple already has data loaded
		 */
		public Row buildData() {
			for(String fieldName: this.schema.fieldNames())
				if(!fields.contains(fieldName))
					throw new IllegalStateException("Field " + fieldName + " has no data");
			Row row= RowFactory.create(result.toArray());
			if(tuple!=null) {
				if (tuple.data!=null && tuple.data.size()>0)
					throw new IllegalStateException("Tuple data has already been loaded");
				tuple.data= row;
			}			
			return row;
		}
		
	}/*class OPLTuple.Builder*/
	
	/**
	 * Provides a means to write a tuple as JSON.
	 * Normally, this method is not called directly; it is used by the OPLCollector.toJSON method.
	 * 
	 * @see com.fasterxml.jackson.databind.ser.std.StdSerializer#serialize(java.lang.Object, com.fasterxml.jackson.core.JsonGenerator, com.fasterxml.jackson.databind.SerializerProvider)
	 * 
	 * @throws RuntimeException - if the data type of a field is not String, Integer, or Double
	 * 
	 * @author bloomj
	 */
	void serialize(JsonGenerator generator) throws IOException {
		generator.writeStartObject();
		StructField field;
		scala.collection.Iterator<StructField> fields= this.schema.iterator();
		while(fields.hasNext()) {
			field= fields.next();
			if(field.dataType().equals(DataTypes.StringType))
				generator.writeStringField(field.name(), this.getStringField(field.name()));
			else if(field.dataType().equals(DataTypes.IntegerType))
				generator.writeNumberField(field.name(), this.getIntField(field.name()));
			else if(field.dataType().equals(DataTypes.DoubleType))
				generator.writeNumberField(field.name(), this.getFloatField(field.name()));
			else
				throw new RuntimeException("Field " + field.name() + " has an unsupported type");
		}/*while*/
		generator.writeEndObject();
		return;
	}/*serialize*/

	/**
	 * Provides a means to read a tuple from JSON.
	 * Normally, this method is not called directly; it is used by the OPLTuple.List.deserialize method.
	 * 
	 * @param name of this tuple (all tuples with the same schema should have the same name)
	 * @param schema of the tuple
	 * 
	 * @throws RuntimeException - if the data type of a field is not String, Integer, or Double
	 */
	public static OPLTuple fromJSON(String name, StructType schema, JsonParser parser)
			throws IOException, JsonProcessingException {
		OPLTuple.Builder builder= new OPLTuple.Builder(schema);
		String fieldName;
		parser.getCurrentToken();	//Start tuple object
		if (parser.getCurrentToken() != JsonToken.START_OBJECT) {
			throw new IOException("Expected data to start with an Object");
		}
		parser.nextValue(); // first field
		while (parser.getCurrentToken() != JsonToken.END_OBJECT) {				
			fieldName = parser.getCurrentName();
			if(schema.apply(fieldName).dataType().equals(DataTypes.StringType))
				builder= builder.add(fieldName, parser.getValueAsString());
			else if(schema.apply(fieldName).dataType().equals(DataTypes.IntegerType))
				builder= builder.add(fieldName, parser.getValueAsInt());
			else if(schema.apply(fieldName).dataType().equals(DataTypes.DoubleType))
				builder= builder.add(fieldName, parser.getValueAsDouble());
			else
				throw new RuntimeException("Field " + fieldName + " has an unsupported type");			

			parser.nextValue(); //next field
		}/*while*/	
		parser.nextToken(); // start next tuple object or end tupleset array
		Row row= builder.buildData();
		return new OPLTuple(name, schema, row);		
	}/*fromJSON*/
	
	/**
	 * This class represents an OPL tupleset as a list-like object. 
	 * (The class is called OPLTuple.List rather than OPLTuple.Set because lists and sets have different properties in Java;
	 * nevertheless, this documentation uses the term "tupleset" to refer to objects of this class.)
	 * This class is a tool for creating a Spark dataset, and it is not intended to serve as the analog of an OPL tupleset.
	 * An OPLTuple.List object is intended for use in creating a Spark dataset from a procedure (e.g. a JSON parser) that generates rows sequentially;
	 * this usage relies on a hack of Spark that may prove fragile in the future. 
	 * Because it wraps an iterable object to look like a list, certain List methods are not supported.
	 * If, in the course of execution, one encounters an UnsupportedOperationException from one of these methods, 
	 * it means that the treatment of the list in the underlying Spark createDataFrame method has changed. A particular concern 
	 * is the treatment of the list's size. Because the rows are streamed, it is usually not possible to know in advance how many rows will be generated. 
	 * The List.iterator methods count the rows as they are generated, so the list's size becomes known only after the iteration has completed.
	 * Calling the size method before then results in an IllegalStateException. Once the rows have been counted, the size of the table 
	 * is accessible through the size method; this can avoid calling the Spark count method whenever possible.
	 * 
	 * @author bloomj
	 *
	 */
	public static class List extends AbstractSequentialList<OPLTuple> {
		
		private Iterable<OPLTuple> tupleset;
		private Iterator<OPLTuple> tupleIterator;
		private StructType schema;
		private Integer size;
			
		/**
		 * Constructs an OPLTuple.List object.
		 * @param tupleIterator an iterator that returns OPLTuple instances (uses tupleset.iterator if not present)
		 * @param schema of each tuple 
		 * @param tupleset an iterable object that returns OPLTuple instances (null if not present)
		 */
		public List(Iterator<OPLTuple> tupleIterator, StructType schema) {
			super();
			this.tupleset= null;
			this.tupleIterator= tupleIterator;
			this.schema= schema;
			this.size= null;		
		}

		public List(Iterable<OPLTuple> tupleset, StructType schema) {
			this(tupleset.iterator(), schema);
			this.tupleset= tupleset;
		}
		
		/**
		 * Returns an empty OPLTuple.List instance.
		 * 
		 * @param schema
		 */
		public List(StructType schema) {
			this(new LinkedList<OPLTuple>(), schema);
		}
		
		/**
		 * Builds a tupleset with programmatically generated data.
		 * Note: the builder creates an actual Java List instance in memory; 
		 * this builder may not be the best way to create a Spark dataset from external data. 
		 * Alternatively, consider using OPLTuple.List.Deserializer with a JSON data source, 
		 * which streams the data from the source into a Spark dataset.
		 * 
		 * @author bloomj
		 *
		 */
		public static class Builder {
			
			private String tableName;
			private String tupleName;
			private java.util.List<OPLTuple> tupleset;
			private StructType schema;
			
			/**
			 * Creates a new OPLTuple.List.Builder instance.
			 * 
			 * @param tableName
			 * @param tupleName all tuples in the set should use the same name (if absent, defaults to "T"+tableName)
			 * @param schema of the tuples in this tupleset
			 */
			public Builder(String tableName, String tupleName, StructType schema) {
				super();
				this.tableName= tableName;
				this.tupleName= tupleName;
				this.schema= schema;
				this.tupleset= new LinkedList<OPLTuple>();
			}
			
			public Builder(String tableName, StructType schema) {
				this(tableName, defaultName(tableName), schema);
			}
			
			public String getTableName() {
				return tableName;
			}
			

			public String getTupleName() {
				return tupleName;
			}

			public StructType getSchema() {
				return schema;
			}

			/**
			 * Adds a tuple to this tupleset.
			 * Note: you can use OPLTuple.Builder to create the tuple.
			 * Note: the schema of the tuple must match that of the tupleset, 
			 * but this method does not check that condition.
			 * 
			 * @param tuple to be added
			 * @return this builder
			 */
			public Builder add(OPLTuple tuple) {
				if(tuple!=null)
					tupleset.add(tuple);
				return this;
			}
			
			/**
			 * Creates and adds a new tuple to this tupleset.
			 * 
			 * @param data to construct a new tuple; uses the schema of the tupleset
			 * @return this builder
			 */
			public Builder add(Object... data) {
				if(data.length>0)
					tupleset.add(new OPLTuple(tupleName, schema, data));
				return this;				
			}
			
			public Builder add(Row data) {
				if(data.length()>0)
					tupleset.add(new OPLTuple(tupleName, schema, data));
				return this;
			}
			
			/**
			 * Completes the build process.
			 * 
			 * @return an OPLTuple.List instance backed by a Java List.
			 */
			public OPLTuple.List buildTupleset() {
				if(tupleset.isEmpty())
					return new OPLTuple.List(schema);
				return new OPLTuple.List(tupleset, schema);
			}
			
		}/*class Builder*/
		
		/**
		 * Wraps a Spark dataset as a OPLTuple List.
		 * Note: this method uses the local iterator of the dataset and 
		 * therefore may be rather costly in terms of accessing the table rows, 
		 * since dataset operations using it cannot be performed in parallel.
		 * It should be used in situations when the operation cannot be serialized and 
		 * distributed among the dataset's partitions.
		 * 
		 * @param tableName
		 * @param df the dataset to be wrapped
		 * @return an OPLTuple.List instance
		 */
		public static OPLTuple.List fromDataset(String tableName, Dataset<Row> df) {
			Iterator<Row> it= df.toLocalIterator();
			return new OPLTuple.List(
				new Iterator<OPLTuple>(){
					@Override public boolean hasNext()	{return it.hasNext();}
					@Override public OPLTuple next()	{return new OPLTuple(defaultName(tableName), df.schema(), it.next());}
					}/*Iterator*/,
				df.schema())/*OPLTuple.List*/;/*return*/
		}
		
		/**
		 * Displays the rows of this list.
		 * 
		 * @param out the destination of the display
		 */
		public void display(PrintStream out) {
			for (OPLTuple tuple: this)
				out.println(tuple.toString());
		}
		
		/**
		 * Displays the schema of this list.
		 * 
		 * @param out the destination of the display
		 */
		public void displaySchema(PrintStream out) {
			out.println(this.schema.toString());
		}
				
		/** 
		 * Returns the size of the list, if it is known.
		 * If the underlying iterable is an instance of Collection, this method returns its size.
		 * Otherwise, the size is not known until the iteration has run, since there is no way to determine the number of tuples generated in advance.
		 * 
		 * @see java.util.AbstractCollection#size()
		 * 
		 * @throws IllegalStateException if the size is not known when this method is called
		 */
		@Override public int size() {
			if(this.tupleset!=null && this.tupleset instanceof Collection)
				return ((Collection<OPLTuple>) tupleset).size();
			if(this.size==null)
				throw new IllegalStateException("size not defined before iterator completes");
			return this.size;
		}
		
		/** 
		 * Returns a ListIterator for this OPLTuple.List.
		 * If the underlying iterable is an instance of java.util.List, this method wraps its ListIterator 
		 * (but does not support the remove, set, or add operations).
		 * Otherwise, it returns an ordinary iterator that implements only those methods specified by the standard Iterator interface (that is, hasNext and next).
		 * It throws UnsupportedOperationException if one of the other ListIterator methods is called.		 
		 *  
		 * @see java.util.AbstractSequentialList#listIterator(int)
		 */
		@Override
		public ListIterator<OPLTuple> listIterator(int index) {		
			if(this.tupleset!=null && this.tupleset instanceof java.util.List)
				return ((java.util.List<OPLTuple>) tupleset).listIterator(index);
			if(index>0)
				throw new IllegalArgumentException("index must be zero");
			return new TupleIterator();
		}
		
		/**
		 * Applies a transformation to each tuple as it it returned.
		 * The transformation is a user-defined class that implements spark.api.java.function.MapFunction 
		 * and transforms an OPLTuple instance into another OPLTuple with a specified schema.
		 * 
		 * @param transformation the transformation class
		 * @param tableName the name of the transformed table
		 * @param resultSchema the schema of the transformed rows
		 * @return an OPLTuple.List
		 */
/*		public OPLTuple.List map(MapFunction<OPLTuple, OPLTuple> transformation, String tableName, StructType resultSchema) {
			return new OPLTuple.List(new TupleIterator(transformation, resultSchema), resultSchema);
		}
*/
		/**
		 * This class wraps the underlying Iterable's iterator methods and counts the tuples as they are generated.
		 * Optionally, it can apply a transformation to the tuples as they are returned.
		 * 
		 * @author bloomj
		 *
		 */
		class TupleIterator implements ListIterator<OPLTuple> {
			
			private int count;
			
			/**
			 * Creates a new iterator instance.
			 * 
			 * @param transformation to be applied to the rows as they are returned (null if absent)
			 */
			TupleIterator() {
				super();
				this.count=0;
			}
			
			@Override 
			public boolean hasNext() {
				return OPLTuple.List.this.tupleIterator.hasNext();
			}/*hasNext*/

			@Override 
			public OPLTuple next() {
				if(!this.hasNext()) 
					 throw new IndexOutOfBoundsException();
				this.count++;
				OPLTuple tuple= OPLTuple.List.this.tupleIterator.next();
				OPLTuple result= tuple;

				if(!this.hasNext())
					OPLTuple.List.this.size= new Integer(this.count);
				return result;
			}/*next*/

			@Override public boolean hasPrevious()		{throw new UnsupportedOperationException();}
			@Override public OPLTuple previous()		{throw new UnsupportedOperationException();}
			@Override public int nextIndex()			{throw new UnsupportedOperationException();}
			@Override public int previousIndex()		{throw new UnsupportedOperationException();}
			@Override public void remove()				{throw new UnsupportedOperationException();}
			@Override public void set(OPLTuple e)		{throw new UnsupportedOperationException();}
			@Override public void add(OPLTuple e)		{throw new UnsupportedOperationException();}

		}/*class OPLTuple.List.TupleIterator*/	
		
		/**
		 * Creates a Spark dataset from this list.
		 * Uses the Spark method SparkSession.createDataFrame(java.util.List<Row> rows, StructType schema).
		 * Because OPLTuple.List wraps an iterable object to look like a list, certain List methods are not supported.
		 * If, in the course of execution, one encounters an UnsupportedOperationException from one of these methods, 
		 * it means that the treatment of the list in the underlying Spark createDataFrame method has changed. 
		 * 
		 * @return a Spark Dataset of Rows.
		 */
		public Dataset<Row> toDataset() {
			
			java.util.ListIterator<OPLTuple> tuples= OPLTuple.List.this.listIterator(0);
			
			/*local*/ class RowIterator extends AbstractSequentialList<Row> {
				
				RowIterator() {super();}
			
				@Override 
				public ListIterator<Row> listIterator(int index) {
					return new ListIterator<Row>(){
						@Override public boolean hasNext() 		{return tuples.hasNext();}
						@Override public Row next() 			{return tuples.next().asRow();}
						@Override public boolean hasPrevious()	{throw new UnsupportedOperationException();}
						@Override public Row previous()			{throw new UnsupportedOperationException();}
						@Override public int nextIndex()		{throw new UnsupportedOperationException();}
						@Override public int previousIndex()	{throw new UnsupportedOperationException();}
						@Override public void remove()			{throw new UnsupportedOperationException();}
						@Override public void set(Row e)		{throw new UnsupportedOperationException();}
						@Override public void add(Row e)		{throw new UnsupportedOperationException();}					
					}/*class ListIterator*/;
				}/*method listIterator()*/

				@Override
				public int size() {
					return OPLTuple.List.this.size;
				}
				
			}/*local class RowIterator*/
			
//			System.out.println("In OPLTuple.List.toDataset: " + OPLTuple.List.this.schema.toString());
			return OPLGlobal.SPARK_SESSION.createDataFrame(new RowIterator(), 
				OPLTuple.List.this.schema);
			
		}/*toDataset*/

		/**
		 * Creates an OPLTuple list representation of a table from JSON.
		 * Note, this is streaming operation that uses the Jackson Streaming API. No intermediate data structures
		 * are created either in memory or as files.
		 * 
		 * @param tupleName
		 * @param schema the application data model entry for this table
		 * @param parser the JSON parser in its current state as passed by the caller
		 * 
		 * @return a Spark data set
		 * @throws IOException or JsonProcessingException when a parse error is encountered
		 */
		@Deprecated
		public static OPLTuple.List deserialize(String tupleName, StructType schema, JsonParser parser)
				throws IOException, JsonProcessingException {
			
			/**
			 * This local class walks through the JSON input and parses each tuple.
			 * 
			 * @author bloomj
			 *
			 */
			/*local*/ class ParsingIterable implements Iterable<OPLTuple> {
				
				ParsingIterable() {
					super();
					if(parser.getCurrentToken()!=JsonToken.START_OBJECT)
						throw new IllegalArgumentException("Expected to start an object");
				}

				@Override
				public Iterator<OPLTuple> iterator() {
					return new Iterator<OPLTuple>(){
						
						@Override
						public boolean hasNext() {
							return (parser.getCurrentToken() != JsonToken.END_ARRAY);
						}
		
						@Override
						public OPLTuple next() {
							if(!this.hasNext()) 
								 throw new IndexOutOfBoundsException();
							OPLTuple tuple= null;
							try {
								tuple = OPLTuple.fromJSON(tupleName, schema, parser);
//								System.out.println("In OPLTuple.List.deserialize 0: " + tuple.toString());
							} catch (IOException e) {
								System.err.println("parse error");
								e.printStackTrace();
							}
							return tuple;
						}/*next*/
						
					}/*local class Iterator*/;
				}/*iterator method*/
				
			}/*local class ParsingIterable*/
						
			//body of deserialize method
//			System.out.println("In OPLTuple.List.deserialize 1: next token= " + parser.getCurrentToken().toString());
			if(!(parser.getCurrentToken()==JsonToken.END_OBJECT || parser.getCurrentToken()==JsonToken.START_ARRAY))
				throw new IOException("Expected to start an array");
			parser.nextToken(); //Start tuple object
//			System.out.println("In OPLTuple.List.deserialize 2: next token= " + parser.getCurrentToken().toString());
			if ((parser.getCurrentToken()) != JsonToken.START_OBJECT) 
				throw new IOException("Expected to start an object");
			OPLTuple.List tupleset= new OPLTuple.List(new ParsingIterable(), schema); 
//			System.out.println("In OPLTuple.List.deserialize 3");
//			tupleset.display(System.out);
			return tupleset;
		}/*deserialize*/
		
		/**
		 * Provides a means to create an OPLTuple set (actually a List) from JSON.
		 * The tupleset's schema corresponding to the JSON input file must already have been created.
		 * It returns an empty tupleset if the JSON array is empty.
		 * 
		 * Optionally, you can apply a transformation to the table's tuples as they are read from the JSON file.
		 * When a transformation has been applied to a table, its output schema replaces the table's input schema
		 * in tupleset.
		 * 
		 * The deserialization implemented in OPLTuple.List is more general than is supported by Spark, in two ways.
		 * It supports other data sources than files (e.g. streams) and supports data streaming without creating intermediate
		 * in-memory or file data stores (note: in this context, streaming is used in the traditional sense, 
		 * not the real-time sense of the Spark Streaming facility).
		 * 
		 * The deserialization is implemented using the Jackson Streaming API, which avoids creating additional in-memory data structures.
		 * 
		 * @param tableName
		 * @param jsonSchema the schema of the tuples to be read from JSON (may differ from the schema of the output if a transformation is used)
		 * @param transformation (optional) to be applied to the table's tuples as they are read from the JSON file (null if not present)
		 * @param parser the JSON parser created by the parent collector's deserializer
		 * @return an iterator over the table's tuples
		 */
		public static OPLTuple.List fromJSON(String tableName, StructType jsonSchema, OPLTuple.List.Deserializer.Transformer transformation, JsonParser parser) {
			OPLTuple.List tupleset;
			OPLTuple.List.Deserializer tupleDeserializer= new OPLTuple.List.Deserializer(OPLTuple.defaultName(tableName), jsonSchema, parser, transformation);
			tupleDeserializer.initialize();
			if(!tupleDeserializer.isEmpty())
				tupleset= new OPLTuple.List(tupleDeserializer, (transformation==null) ? jsonSchema : transformation.getOutputSchema());
			else
				tupleset= new OPLTuple.List(jsonSchema); //empty tupleset			
			return tupleset;		
		}/*fromJSON*/
		
		public static OPLTuple.List fromJSON(String tableName, StructType jsonSchema, JsonParser parser) {
			return OPLTuple.List.fromJSON(tableName, jsonSchema, null, parser);
		}
		
			
		/**
		 * Creates an OPLTuple set (actually a List) representation of a table from JSON.
		 * This class walks through the JSON input and parses each tuple, using the jsonSchema 
		 * corresponding to the JSON input file, which must already have been created. 
		 * 
		 * Optionally, you can apply a transformation to a table's tuples as they are read from the JSON file.
		 * When a transformation has been applied to a table, its output schema replaces the table's input jsonSchema.
		 * 
		 * The OPLTuple list deserializer is intended to be called by the OPLCollector
		 * deserializer and is not intended to be used independently. The OPLCollector
		 * class has a one-to-one correspondence between an OPLCollector instance 
		 * and its JSON representation.
		 * 
		 * The deserialization implemented in OPLTuple.List is more general than is supported by Spark.
		 * It supports other data sources than files (e.g. streams) and supports data streaming without creating intermediate
		 * in-memory or file data stores (note: in this context, streaming is used in the traditional sense, 
		 * not the real-time sense of the Spark Streaming facility).
		 * 
		 * The deserialization is implemented using the Jackson Streaming API, which avoids creating additional in-memory data structures.
		 * 
		 * Note, this is streaming operation that uses the Jackson Streaming API. No intermediate data structures
		 * are created either in memory or as files.
		 * 
		 * @author bloomj
		 *
		 */
		public static class Deserializer implements Iterator<OPLTuple> {
			
			private String tupleName;
			private StructType jsonSchema;
			private JsonParser parser;
			private boolean isInitialized;
			private boolean isEmpty;
			private Transformer transformation;
			
			/**
			 * Creates a new deserializer instance.
			 * 
			 * @param tupleName (usually defaults to the table name with "T" prepended)
			 * @param jsonSchema the schema of the tuples to be read from JSON (may differ from the schema of the output if a transformation is used)
			 * @param parser the JSON parser created by the collector deserializer
			 * @param transformation (optional) to be applied to the table's tuples as they are read from the JSON file (null if not present)
			 */
			public Deserializer(String tupleName, StructType jsonSchema, JsonParser parser) {
				super();
				this.tupleName= tupleName; 
				this.jsonSchema= jsonSchema;
				this.parser= parser;
				this.transformation= null;
				this.isInitialized= false;
				this.isEmpty= true;
			}
			
			public Deserializer(String tupleName, StructType jsonSchema, JsonParser parser, Transformer transformation) {
				this(tupleName, jsonSchema, parser);
				this.transformation= transformation;
			}
					
			/**
			 * Initializes the deserializer and positions the JSON parser at the start of the first tuple object.
			 * Detects an empty JSON array.
			 * Note, initialize must be called before calling isEmpty, hasNext, or next; otherwise they will throw an exception.
			 * 
			 * @throws IOException if the parser cannot be properly positioned
			 */
			public void initialize() {
				try {
					if(!(this.parser.getCurrentToken()==JsonToken.END_OBJECT || this.parser.getCurrentToken()==JsonToken.START_ARRAY))
							throw new IOException("Expected to start an array");
					this.parser.nextToken(); //Start tuple object
					this.isEmpty= this.parser.getCurrentToken()==JsonToken.END_ARRAY;
	//				System.out.println("In OPLTuple.List.deserialize 2: next token= " + parser.getCurrentToken().toString());
					this.isInitialized= true;
				} catch (IOException e) {
					System.err.println("JSON parsing error: " + e.getMessage());
					e.printStackTrace();
				}
			}
			
			/**
			 * @return true unless the parser has reached the end of the tuple array
			 * @throws IllegalStateException if the deserializer has not been initialized
			 * @see java.util.Iterator#hasNext()
			 */
			@Override
			public boolean hasNext() {
				if(!isInitialized)
					throw new IllegalStateException("Deserializer must be intialized");
				if(this.isEmpty())
					return false;
				return (this.parser.getCurrentToken() != JsonToken.END_ARRAY);
			}

			/**
			 * Reads the next tuple object from the JSON file.
			 * Applies a transformation if one has been specified, and 
			 * after the last tuple has been read, replaces the input jsonSchema with the output schema.
			 * 
			 * @throws IOException if the parser encounters an error
			 * @see java.util.Iterator#next()
			 */
			@Override
			public OPLTuple next() {
				if(!this.hasNext()) 
					 throw new NoSuchElementException();
				OPLTuple tuple= null;
				try {
					tuple = OPLTuple.fromJSON(this.tupleName, this.jsonSchema, this.parser);
//					System.out.println("In OPLTuple.List.deserialize 0: " + tuple.toString());
				} catch (IOException e) {
					System.err.println("Parse error: " + e.getMessage());
					e.printStackTrace();
				}
				
				if(this.transformation!=null) {
					tuple= transformation.call(tuple);
				}
				
				return tuple;
			}
			
			/**
			 * Checks whether the tupleset generated by this Deserializer has any tuples in it.
			 * 
			 * @return true is the tupleset contains no tuples, true otherwise
			 * @throws IllegalStateException if the deserializer has not been initialized
			 */
			public boolean isEmpty() {
				if(!this.isInitialized)
					throw new IllegalStateException("Deserializer must be intialized");
				return this.isEmpty;
			}
			
			/**
			 * @return the output schema of this deserializer 
			 * (the output schema of the transformation is there is one; otherwise, the input schema)
			 */
			public StructType getSchema() {
				if(this.transformation!=null)
					return transformation.getOutputSchema();
				/*else*/return this.jsonSchema;
			}
			
			/**
			 * This abstract class applies a transformation to a tuple as it is read by the deserializer.
			 * 
			 * The programmer must define the transformation by overriding the abstract call method.
			 * 
			 * The transformed tuple must be specified by an output schema; it is the programmers responsibility
			 * to modify the parent collector's application data model to reflect the output schema, 
			 * which can typically be done in the transformation's constructor.
			 * 
			 * Transformations are useful when they are simple, and the Spark map operation might be difficult to apply because
			 * the transformation cannot be serialized. However, since it operates in parallel, using map may be more efficient 
			 * for complex transformations. 
			 * 
			 * @author bloomj
			 *
			 */
			public static abstract class Transformer implements org.apache.spark.api.java.function.MapFunction<OPLTuple, OPLTuple> {
				
				private static final long serialVersionUID = -8523362485367573681L;
				
				private String tableName;
				private StructType outputSchema;

				/**
				 * Creates a new transformer instance.
				 * The programmer should call this as the super() constructor in her subclass.
				 * 
				 * @param tableName affected by the transformation; 
				 * must correspond to one of the tables in the parent collector.
				 * @param outputSchema schema of the transformed tuples
				 */
				public Transformer(String tableName, StructType outputSchema) {
					super();
					this.tableName= tableName;
					this.outputSchema= outputSchema;
				};

				public String getTableName() {
					return tableName;
				}

				public StructType getOutputSchema() {
					return outputSchema;
				}

				/**
				 * Performs the actual transformation from input tuple to output tuple.
				 * The programmer must override this abstract method.
				 * 
				 * @see org.apache.spark.api.java.function.MapFunction#call(java.lang.Object)
				 */
				public abstract OPLTuple call(OPLTuple input);
		
			}/*class OPLTuple.List.Deserializer.Transformer*/
			
		}/*class OPLTuple.List.Deserializer*/
		
	}/*class OPLTuple.List*/;
	
	
}/*class OPLTuple*/
