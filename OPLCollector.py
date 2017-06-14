__author__ = 'bloomj'

'''
Created on Feb 8, 2017
@author: bloomj
'''
import sys
import os
import json
import requests

try:
    from pyspark import SparkConf, SparkContext
    from pyspark.sql import SparkSession, Row, functions
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
except ImportError as e:
    print ("Error importing Spark Modules", e)
    sys.exit(1)

SPARK_CONTEXT = SparkContext(conf=SparkConf().setAppName("OPL").setMaster("local[4]"))
#SQL_CONTEXT = sqlContext  # sqlContext is predefined
SPARK_SESSION = SparkSession.builder.config("spark.sql.crossJoin.enabled", "true").getOrCreate()


class OPLCollector(object):
    '''
    Represents an OPL data model in Spark.
    Note: Use of this class does not depend on OPL, and in particular, it can be used with the DOcplex Python API.
    An application data model (ADM) consists of a set of tables (OPL Tuplesets), each with its own schema.
    An ADM is represented by a dictionary in which the keys are the table names and the values are the table schemas.
    A builder is provided to create the ADM.

    The OPLCollector holds the actual data in Spark Datasets. There are several ways to populate
    the data.
    - Spark SQL operations can transform tables into other tables.
    - A builder is provided when the data is generated programmatically.
    - JSON deserialization and serialization are provided when data is exchanged with external applications or stores.

    The design of the OPLCollector class aims to reduce the amount of data that must be
    manipulated outside of Spark. Where possible, data is streamed among applications without
    creating auxiliary in-memory structures or files.

    The design of OPLCollector also aims to minimize the amount
    of custom coding required to build an application. Collectors are configured
    by specifying their schemas through builders rather than by extending with subclasses.
    '''

    def __init__(self, collectorName, applicationDataModel={}, sparkData={}):
        '''
        Creates a new OPLCollector instance.

        :param collectorName: the name of this collector.
        :type collectorName: String
        :param applicationDataModel: holds the table schemas for this collector. Each schema is a Spark StructType.
        Note that each collector has one and only one application data model.
        :type applicationDataModel: dict<String, StructType>
        :param sparkData: holds the actual data tables of this collector as a set of Spark datasets.
        :type sparkData: dict<String, Dataframe>
        '''
        self.name = collectorName
        self.applicationDataModel = applicationDataModel
        self.sparkData = sparkData
        self.size = {name: None for name in applicationDataModel.keys()}
        self.jsonDestination = None
        self.jsonSource = None

    def copy(self, name):
        """
        Creates a new OPLCollector instance with copies of the application data model and Spark datasets of this collector.
        The ADM copy is immutable. The Spark datasets themselves are immutable, but the copy supports the addTable, addData, and replaceTable methods.
        Does not copy the JSONSource or JSONDestination fields.

        :param name of the new collector
        :param tableNames tables to be copied (all tables in this collector, if absent)
        :return a new OPLCollector instance
        """
        result = OPLCollector(name, self.applicationDataModel, self.sparkData.copy())
        result.size = self.size.copy()
        return result

    def copy(self, name, *tables):
        result = OPLCollector(name)
        admBuilder = ADMBuilder(result);
        for table in tables:
            admBuilder.addSchema(table, self.getSchema(table))
        admBuilder.build()
        dataBuilder = DataBuilder(result.applicationDataModel, collector=result)
        for table in tables:
            dataBuilder.addTable(table, self.getTable(table))
            result.size[table] = self.size[table]
        dataBuilder.build();
        return result

    def getName(self):
        """
        Returns the name of this collector.

        :return collector name as a string
        """
        return self.name

    def addTables(self, other):
        """
        Adds a set of tables of data from another collector.
        An individual table can be set only once.

        :param other: another collector
        :type other: OPLCollector
        :raise ValueError: if the other ADM is empty or if a table name duplicates a name already present in this collector.
        """

        if not other.applicationDataModel:  # is empty
            raise ValueError("empty collector")
        for tableName in other.applicationDataModel.viewkeys():
            if tableName in self.applicationDataModel:
                raise ValueError("table " + tableName + " has already been defined")
        self.applicationDataModel.update(other.applicationDataModel)
        self.sparkData.update(other.sparkData)
        self.size.update(other.size)
        return self

    def replaceTable(self, tableName, table, size=None):
        """
        Replaces an individual table of data.

        :param tableName:
        :type String
        :param table:
        :type Spark Dataframe
        :param size: number of rows in table (None if omitted)
        :return: this collector
        :raise ValueError: if the table is not already defined in the ADM
        """
        if tableName not in self.applicationDataModel:
            raise ValueError("table " + tableName + "has not been defined")
        self.sparkData[tableName] = table
        if size is not None:
            self.size[tableName] = size
        else:
            self.size[tableName] = table.count()
        return None

    def addData(self, tableName, table, size=None):
        """
        Adds data to an existing table.
        Use when a table has several input sources.
        Does not deduplicate the data (i.e. allows duplicate rows).

        :param tableName:
        :type String
        :param table:
        :type Spark Dataframe
        :param size: number of rows in table (None if omitted)
        :return: this collector
        :raise ValueError: if the table is not already defined in the ADM
        """
        if tableName in self.applicationDataModel:
            raise ValueError("table " + tableName + " has already been defined")
        self.sparkData[tableName] = self.sparkData[tableName].union(table)
        count = (self.size[tableName] + size) if (self.size[tableName] is not None and size is not None) else None
        self.size[tableName] = count
        return self

    #NEW
    def getADM(self):
        """
        Exposes the application data model for this OPLCollector.
        The ADM is represented by a map in which the keys are the table names
        and the values are the table schemas held in Spark StructType objects.

        :return: the application data model
        :rtype: dict<String, StructType>
        """
        return self.applicationDataModel

    def setADM(self, applicationDataModel):
        """
        Sets the application data model for this OPLCollector.
        The ADM cannot be changed once set.
        """
        if (self.applicationDataModel):  # is not empty or None
            raise ValueError("ADM has already been defined")
        self.applicationDataModel = applicationDataModel
        return self

    def getTable(self, tableName):
        return self.sparkData[tableName]

    def getSchema(self, tableName):
        return self.applicationDataModel[tableName]

    def selectSchemas(self, *tableNames):
        """
        Returns a subset of the application data model.
        """
        return {tableName: self.applicationDataModel[tableName] for tableName in tableNames}

    def selectTables(self, collectorName, *tableNames):
        """
        Creates a new OPLCollector from a subset of the tables in this collector.
        The tables in the new collector are copies of the tables in the original.
        """
        adm = self.selectSchemas(tableNames)
        data = {tableName: SPARK_SESSION.createDataFrame(self.sparkData[tableName], self.getSchema(tableName))
                for tableName in tableNames}
        size = {tableName: self.size[tableName] for tableName in tableNames}
        return OPLCollector(collectorName, adm, data, size)

    def getSize(self, tableName):
        """
        Returns the number of rows in a table.
        Note: the Spark data set count method is fairly expensive,
        so it is used only if there is no other way to count the number of rows.
        It is best to count the rows as the table is being deserialized, as is done in the fromJSON method.
        Once counted, the number is stored in the size map for future use.
        """
        if tableName not in self.size:
            raise ValueError("size not defined for table " + tableName)
        if self.size[tableName] is None:
            self.size[tableName] = self.sparkData[tableName].count()
        return self.size[tableName]

    def buildADM(self):
        """
        Creates the application data model for this collector
        """
        if (self.applicationDataModel):  # is not empty
            raise ValueError("application data model has already been defined")
        return ADMBuilder(self)

    def buildData(self):
        """
        Creates a builder for the data tables for this collector.
        Uses this collector's application data model.

        :return: a new DataBuilder instance
        :raise ValueError: if the application data model has not been defined or if data tables have already been loaded
        """
        if not self.applicationDataModel:  # is empty
            raise ValueError("application data model has not been defined")
        if self.sparkData:  # is not empty
            raise ValueError("data tables have already been loaded")
        return DataBuilder(self.applicationDataModel, collector=self)

    def setJsonSource(self, source):
        """
        Sets the source for the JSON text that populates the collector.
        There is a one-to-one correspondence between an OPLCollector instance and its JSON representation;
        that is, the JSON source file must fully include all the data tables to be populated in the collector instance.
        Thus, it makes no sense to have more than on JSON source for a collector or to change JSON sources.

        :param source: a file-like object containing the JSON text.
        :return: this collector instance
        :raise ValueError: if JSON source has already been set
        """
        if self.jsonSource is not None:
            raise ValueError("JSON source has already been set")
        self.jsonSource = source
        return self

    #REVISED
    def fromJSON(self):
        """
        Provides a means to create a collector from JSON.
        You must first set the destination (an output stream, file, url, or string) where the JSON will be read.
        Then you call the deserializer fromJSON method.
        The application data model for the collector must already have been created.

        There is a one-to-one correspondence between an OPLCollector instance and its JSON representation;
        that is, the JSON source file must fully include all the data tables to be populated in the collector instance.
        Methods are provided to merge two collectors with separate JSON sources (addTables),
        add a data set to a collector (addTable), and to add data from a data set to an existing table in a collector.

        :return: this collector with its data tables filled
        :raise ValueError: if the data tables have already been loaded
        """
        if self.sparkData:  # is not empty
            raise ValueError("data tables have already been loaded")
        # data: dict {tableName_0: [{fieldName_0: fieldValue_0, ...}, ...], ...}
        data = json.load(self.jsonSource)
        builder = self.buildData()
        for tableName, tableData in data.viewitems():
            count = len(tableData)
            tableRows = (Row(**fields) for fields in tableData)
            builder = builder.addTable(tableName,
                                       SPARK_SESSION.createDataFrame(tableRows, self.getADM()[tableName]),
                                       count)   # would like to count the rows as they are read instead,
                                                # but don't see how
        builder.build()
        return self

    def setJsonDestination(self, destination):
        """
        Sets the destination for the JSON serialization.
        Replaces an existing destination if one has been set previously.

        :param destination: an output string, stream, file, or URL
        :return: this collector
        """
        self.jsonDestination = destination
        return self

    #REVISED
    def toJSON(self):
        """
        Provides a means to write the application data as JSON.
        You must first set the destination (an output stream, file, url, or string) where the JSON will be written.
        Then you call the serializer toJSON method.
        """
        self.jsonDestination.write("{\n")                           # start collector object
        firstTable = True
        for tableName in self.sparkData:
            if not firstTable:
                self.jsonDestination.write(',\n')
            else:
                firstTable = False
            self.jsonDestination.write('"' + tableName + '" : [\n') # start table list
            firstRow = True
            for row in self.sparkData[tableName].toJSON().collect():# better to use toLocalIterator() but it gives a timeout error
                if not firstRow:
                    self.jsonDestination.write(",\n")
                else:
                    firstRow= False
                self.jsonDestination.write(row)                     # write row object

            self.jsonDestination.write("\n]")                       # end table list
        self.jsonDestination.write("\n}")                           # end collector object

    #REVISED
    def displayTable(self, tableName, out=sys.stdout):
        """
        Prints the contents of a table.

        :param tableName: String
        :param out: a file or other print destination where the table will be written
        """
        out.write("collector: " + self.getName() + "\n")
        out.write("table: " + tableName + "\n")
        self.getTable(tableName).show(self.getSize(tableName), truncate=False)

    # REVISED
    def display(self, out=sys.stdout):
        """
        Prints the contents of all tables in this collector.

        :param out: a file or other print destination where the tables will be written
        """
        for tableName in self.sparkData:
            self.displayTable(tableName, out=out)


# end class OPLCollector

def getFromObjectStorage(credentials, container=None, filename=None):
    """
    Returns a stream containing a file's content from Bluemix Object Storage.

    :param credentials a dict generated by the Insert to Code  service of the host Notebook
    :param container the name of the container as specified in the credentials (defaults to the credentials entry)
    :param filename the name of the file to be accessed (note: if there is more than one file in the container,
    you might prefer to enter the names directly; otherwise, defaults to the credentials entry)
    """

    if not container:
        container = credentials['container']
    if not filename:
        filename = credentials['filename']

    url1 = ''.join([credentials['auth_url'], '/v3/auth/tokens'])
    data = {'auth': {'identity': {'methods': ['password'],
                                  'password': {
                                      'user': {'name': credentials['username'], 'domain': {'id': credentials['domain_id']},
                                               'password': credentials['password']}}}}}
    headers1 = {'Content-Type': 'application/json'}
    resp1 = requests.post(url=url1, data=json.dumps(data), headers=headers1)
    resp1_body = resp1.json()
    for e1 in resp1_body['token']['catalog']:
        if (e1['type'] == 'object-store'):
            for e2 in e1['endpoints']:
                if (e2['interface'] == 'public' and e2['region'] == credentials['region']):
                    url2 = ''.join([e2['url'], '/', container, '/', filename])
    s_subject_token = resp1.headers['x-subject-token']
    headers2 = {'X-Auth-Token': s_subject_token, 'accept': 'application/json'}
    resp2 = requests.get(url=url2, headers=headers2, stream=True)
    return resp2.raw


class DataBuilder(object):
    """
        Builds the Spark datasets to hold the application data.
        Used when the data are created programmatically.
    """

    def __init__(self, applicationDataModel, collector=None):
        """
        Creates a builder for loading the Spark datasets.

        :param applicationDataModel
        :param collector: if present, loads the data tables and their sizes directly into the collector;
        if not present or null, the Spark data dict is returned directly
        :return: a new DataBuilder instance
        :raise ValueError: if the application data model has not been defined
        """
        if not applicationDataModel:  # is empty
            raise ValueError("application data model has not been defined")
        self.applicationDataModel = applicationDataModel
        self.collector = collector
        self.result = {}
        self.length = {}

    def addTable(self, tableName, data, size=None):
        """
        Get the external data and create the corresponding application dataset.
        Assumes that the schema of this table is already present in the ADM.

        :param data: a Spark dataset
        :param size: length number of rows in table (null if omitted)
        :return this builder instance
        :raise ValueError: if the table is not included in the ADM or if the table has already been loaded
        """
        if tableName not in self.applicationDataModel:
            raise ValueError("table " + tableName + " has not been defined")
        if tableName in self.result:
            raise ValueError("table " + tableName + " has already been loaded")
        self.result[tableName] = data
        self.length[tableName] = size
        return self

    def copyTable(self, tableName, data, size=None):
        return self.addTable(tableName,
                             SPARK_SESSION.createDataFrame(data.rdd()), size);

    def addEmptyTable(self, tableName):
        return self.addTable(tableName,
                             SPARK_SESSION.createDataFrame(SPARK_CONTEXT.emptyRDD(),
                                                           self.applicationDataModel[tableName]), 0)
    #NEW
    def referenceTable(self, tableName):
        """
		Enables referring to a table in the collector under construction to create a new table.
		Can be used in SQL statements.

        :param tableName:
        :type tableName: String
        :return:
        :rtype:
        """
        if tableName not in self.result:
            raise ValueError(tableName + " does not exist")
        return self.result.get(tableName)

    def build(self):
        """
        Completes building the Spark data.
        Registers the application data sets as Spark SQL tables.
        If an OPLCollector has been supplied in the constructor, loads the data tables and their sizes into it.

        :return a dict of table names to Spark data sets containing the application data
        :raise ValueError:  if a table in the ADM has no associated data or if data tables have already been loaded into the collector
        """
        for tableName in self.applicationDataModel:
            if tableName not in self.result:
                raise ValueError("table " + tableName + "has no data")
        for tableName in self.result:
            self.result[tableName].createOrReplaceTempView(tableName)
        if self.collector is not None:
            if self.collector.sparkData:  # is not empty
                raise ValueError("data tables have already been loaded")
            self.collector.sparkData = self.result
            self.collector.size = self.length
        return self.result

    def retrieveSize(self):
        """
        :return the size dict created by this builder
        Note: calling this method before the build method could return an inaccurate result
        """
        return self.length


# end class DataBuilder

class ADMBuilder(object):
    """
    Builds an Application Data Model that associates a set of Spark Datasets with their schemas.
    Usage:

    adm= ADMBuilder()\
        .addSchema("warehouse", buildSchema(
            ("location", StringType()),
            ("capacity", DoubleType()))\
        .addSchema("route", buildSchema(
            ("from", StringType()),
            ("to", StringType()),
            ("capacity", DoubleType()))\
        .build();
    """

    def __init__(self, collector=None):
        """
        Creates a new builder.
        :param collector if present, loads the application data model directly into the collector;
        if not present or null, the ADM map is returned directly
        """
        self.collector = collector
        self.result = {}

    def addSchema(self, tableName, tupleSchema):
        """
        Adds a new table schema to the ADM.

        :param tupleSchema can be built with the buildSchema function
        :return this builder
        :raise ValueError: if a schema for tableName has already been defined
        """
        if tableName in self.result:
            raise ValueError("tuple schema " + tableName + " has already been defined")
        self.result[tableName] = tupleSchema
        return self

        #NEW
        def referenceSchema(self, tableName):
            """
            Enables referring to a schema in the ADM under construction to create a  new schema.

            :param tableName
            :return the schema
            """
            if tableName not in self.result:
                raise ValueError("tuple schema " + tableName + " does not exist")
            return self.result[tableName]

        def build(self):
            """
            Completes building the application data model.
            If an OPLCollector has been supplied in the constructor, loads the ADM into it.

            :return the ADM
            :raise ValueError: if the ADM for the collector has already been defined
            """
            if self.collector is not None:
                if self.collector.applicationDataModel:  # is not empty
                    raise ValueError("application data model has already been defined")
                self.collector.applicationDataModel = self.result
            return self.result

# end class ADMBuilder

def buildSchema(*fields):
    """
    Creates a schema from a list field tuples
    The resulting schema is an instance of a Spark StructType.
    :param fields:
    :type fields: tuple<String, DataType>
    :return:
    :rtype: StructType
    """
    schema = StructType()
    for fieldName, fieldType in fields:
        schema = schema.add(fieldName, fieldType, False, None)
    return schema
# end buildSchema

#NEW
class SchemaBuilder:
    """
    Builds a tuple schema.
    Strictly speaking, this builder is not needed since the StructType class provides the necessary functionality.
    However, it is provided as a convenience.
    Only the following data types are supported in the schema: String, integer, float (represented as Double), and 1-dimensional arrays of integer or float.
    The array types are supported only for internal use and cannot be serialized to or deserialized from JSON.
    Note, the fields in the resulting schema are sorted in dictionary order by name to insure correct matching with data elements.

    Usage:
        StructType warehouseSchema= (new OPLTuple.SchemaBuilder()).addField("location", DataTypes.StringType).addField("capacity", DataTypes.DoubleType).buildSchema();
    The fields of the resulting StructType are not nullable and have no metadata.
    """

    def __init__(self):
        # fields is a dictionary<String, StructField>
        self.fields= {}

    def addField(self, fieldName, fieldType):
        if self.fields.has_key(fieldName):
            raise ValueError("field " + fieldName + " has already been set")
        self.fields[fieldName]= StructField(fieldName, fieldType, False)
        return self;

    def addFields(self, *fields):
        """
        Adds fields from a list field tuples
        :param fields: tuple<String, DataType>
        :return: StructType
        """
        for field in fields:
            fieldName, fieldType= field
            self.addField(fieldName, fieldType)
        return self

    def copyField(self, otherSchema, fieldName):
        """
        Copies fields from another schema
        :param otherSchema: StructType
        :param fieldName: String
        :return: StructType
        """
        if self.fields.has_key(fieldName):
            raise ValueError("field " + fieldName + " has already been set")
        self.fields[fieldName]= otherSchema[fieldName]
        return self

    def copyFields(self, otherSchema):
        for fieldName in otherSchema.names:
            self.copyField(otherSchema, fieldName)
        return self

    def buildSchema(self):
        return StructType(self.fields.values())
# end class SchemaBuilder

