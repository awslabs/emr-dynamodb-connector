# emr-dynamodb-connector
**Access data stored in Amazon DynamoDB with Apache Hadoop, Apache Hive, and Apache Spark**

## Introduction
You can use this connector to access data in Amazon DynamoDB using Apache Hadoop, Apache Hive, and
Apache Spark in Amazon EMR. You can process data directly in DynamoDB using these frameworks, or
join data in DynamoDB with data in Amazon S3, Amazon RDS, or other storage layers that can be
accessed by Amazon EMR.

- [Using Apache Hive in Amazon EMR with Amazon DynamoDB][emr-dynamodb-hive-docs]
- [Accessing data in Amazon DynamoDB with Apache Spark][dynamodb-spark-blog-post]

Currently, the connector supports the following data types:

| Hive type | Default DynamoDB type | Alternate DynamoDb type(s) |
| --- | --- | --- |
| string | string (S) | |
| bigint or double | number (N) | |
| binary | binary (B) | |
| boolean | boolean (BOOL) | |
| array | list (L) | number set (NS), string set (SS), binary set (BS) |
| map<string,string> | item (ITEM) | map (M) |
| map<string,?> | map (M) | |
| struct | map (M) | |

The connector can serialize null values as DynamoDB null type (NULL).

### Hive StorageHandler Implementation
For more information, see [Hive Commands Examples for Exporting, Importing, and Querying Data in
DynamoDB][hive-commands-emr-dev-guide] in the *[Amazon DynamoDB Developer Guide][dynamodb-dev-guide]*.

### Hadoop InputFormat and OutputFormat Implementation
An implementation of [Apache Hadoop InputFormat interface][input-format-javadoc] and
[OutputFormat][output-format-javadoc] are included, which allows
[DynamoDB AttributeValues][dynamodb-attributevalues] to be directly ingested by MapReduce jobs. For
an example of how to use these classes, see
[Set Up a Hive Table to Run Hive Commands][set-up-hive-table] in the
*[Amazon EMR Release Guide][emr-release-guide]*, as well as their usage in the Import/Export tool
classes in [DynamoDBExport.java][export-tool-source] and [DynamoDBImport.java][import-tool-source].

### Import/Export Tool
This simple tool that makes use of the InputFormat and OutputFormat implementations provides an easy
 way to import to and export data from DynamoDB.

### Supported Versions
Currently the project builds against Hive 2.3.0, 1.2.1, and 1.0.0. Set this by using the `hive1.version`,
`hive1.2.version` and `hive2.version` properties in the root Maven `pom.xml`, respectively.

## How to Build
After cloning, run `mvn clean install`.

## Example: Hive StorageHandler
Syntax to create a table using the DynamoDBStorageHandler class:
```
CREATE EXTERNAL TABLE hive_tablename (
    hive_column1_name column1_datatype,
    hive_column2_name column2_datatype
)
STORED BY 'org.apache.hadoop.hive.dynamodb.DynamoDBStorageHandler'
TBLPROPERTIES (
    "dynamodb.table.name" = "dynamodb_tablename",
    "dynamodb.column.mapping" =
        "hive_column1_name:dynamodb_attribute1_name,hive_column2_name:dynamodb_attribute2_name",
    "dynamodb.type.mapping" =
        "hive_column1_name:dynamodb_attribute1_type_abbreviation",
    "dynamodb.null.serialization" = "true"
);
```

`dynamodb.type.mapping` and `dynamodb.null.serialization` are optional parameters.

Hive query will automatically choose the most suitable secondary index if there is any based on the
search condition. For an index that can be chosen, it should have following properties:
1. It has all its index keys in Hive query search condition;
2. It contains all the DynamoDB attributes mentioned in `dynamodb.column.mapping`. (If you have to
map more columns than index attributes in your Hive table but still want to use an index when
running queries that only select the attributes within that index, consider create another
Hive table and narrow down the mappings to only include the index attributes. Use that table for
reading the index attributes to reduce table scans)

## Example: Input/Output Formats with Spark
Using the DynamoDBInputFormat and DynamoDBOutputFormat classes with `spark-shell`:
```
$ spark-shell --jars /usr/share/aws/emr/ddb/lib/emr-ddb-hadoop.jar
...
import org.apache.hadoop.io.Text;
import org.apache.hadoop.dynamodb.DynamoDBItemWritable
import org.apache.hadoop.dynamodb.read.DynamoDBInputFormat
import org.apache.hadoop.dynamodb.write.DynamoDBOutputFormat
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.io.LongWritable

var jobConf = new JobConf(sc.hadoopConfiguration)
jobConf.set("dynamodb.input.tableName", "myDynamoDBTable")

jobConf.set("mapred.output.format.class", "org.apache.hadoop.dynamodb.write.DynamoDBOutputFormat")
jobConf.set("mapred.input.format.class", "org.apache.hadoop.dynamodb.read.DynamoDBInputFormat")

var orders = sc.hadoopRDD(jobConf, classOf[DynamoDBInputFormat], classOf[Text], classOf[DynamoDBItemWritable])

orders.count()
```

## Example: Import/Export Tool
##### Export usage
```
java -cp target/emr-dynamodb-tools-4.2.0-SNAPSHOT.jar org.apache.hadoop.dynamodb.tools.DynamoDBExport /where/output/should/go my-dynamo-table-name
```
##### Import usage
```
java -cp target/emr-dynamodb-tools-4.2.0-SNAPSHOT.jar org.apache.hadoop.dynamodb.tools.DynamoDBImport /where/input/data/is my-dynamo-table-name
```

#### Additional options
```
export <path> <table-name> [<read-ratio>] [<total-segment-count>]

read-ratio: maximum percent of the specified DynamoDB table's read capacity to use for export

total-segments: number of desired MapReduce splits to use for the export
```

```
import <path> <table-name> [<write-ratio>]

write-ratio: maximum percent of the specified DynamoDB table's write capacity to use for import
```

## Maven Dependency
To depend on the specific components in your projects, add one (or both) of the following to your
`pom.xml`.

#### Hadoop InputFormat/OutputFormats & DynamoDBItemWritable
```
<dependency>
  <groupId>com.amazon.emr</groupId>
  <artifactId>emr-dynamodb-hadoop</artifactId>
  <version>4.2.0</version>
</dependency>
```
#### Hive SerDes & StorageHandler
```
<dependency>
  <groupId>com.amazon.emr</groupId>
  <artifactId>emr-dynamodb-hive</artifactId>
  <version>4.2.0</version>
</dependency>
```

## Contributing
* **If you find a bug or would like to see an improvement, open an issue.**

    Check first to make sure there isn't one already open. We'll do our best to respond to issues
    and review pull-requests

* **Want to fix it yourself? Open a pull request!**

    If adding new functionality, include new, passing unit tests, as well as documentation. Also
    include a snippet in your pull request showing that all current unit tests pass. Tests are ran
    by default when invoking any goal for maven that results in the `package` goal being executed
    (`mvn clean install` will run them and produce output showing such).

* **Follow the [Google Java Style Guide][google-style-guide]**

    Style is enforced at build time using the [Apache Maven Checkstyle Plugin][maven-checkstyle-plugin].

[emr-release-guide]: http://docs.aws.amazon.com/ElasticMapReduce/latest/ReleaseGuide/emr-release-components.html
[dynamodb-dev-guide]: http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Introduction.html
[hive-commands-emr-dev-guide]: http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/EMR_Hive_Commands.html
[set-up-hive-table]: http://docs.aws.amazon.com/ElasticMapReduce/latest/ReleaseGuide/EMR_Interactive_Hive.html
[hive-dynamodb-data-types]: http://docs.aws.amazon.com/ElasticMapReduce/latest/ReleaseGuide/EMR_Interactive_Hive.html#EMR_Hive_Properties
[dynamodb-spark-blog-post]: https://blogs.aws.amazon.com/bigdata/post/Tx1G4SQRV049UL0/Analyze-Your-Data-on-Amazon-DynamoDB-with-Apache-Spark
[emr-dynamodb-hive-docs]: http://docs.aws.amazon.com/ElasticMapReduce/latest/ReleaseGuide/EMRforDynamoDB.html
[input-format-javadoc]: https://hadoop.apache.org/docs/current/api/org/apache/hadoop/mapred/InputFormat.html
[output-format-javadoc]: https://hadoop.apache.org/docs/current/api/org/apache/hadoop/mapred/OutputFormat.html
[dynamodb-attributevalues]: http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_AttributeValue.html
[export-tool-source]: emr-dynamodb-tools/src/main/java/org/apache/hadoop/dynamodb/tools/DynamoDBExport.java
[import-tool-source]: emr-dynamodb-tools/src/main/java/org/apache/hadoop/dynamodb/tools/DynamoDBImport.java
[google-style-guide]: https://google.github.io/styleguide/javaguide.html
[maven-checkstyle-plugin]: https://maven.apache.org/plugins/maven-checkstyle-plugin/index.html
