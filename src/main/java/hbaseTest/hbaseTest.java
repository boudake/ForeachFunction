package hbaseTest;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
//import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog;

import java.util.HashMap;
import java.util.Map;
//C:\Users\ddiop\IdeaProjects\myproject\src\main\java\hbaseTest\hbaseTest.java
public class hbaseTest {
    public static void main(String[] args) {

        /*
        SparkSession sparkSession = SparkSession.builder().
                master("local")
                .appName("spark session example")
                .getOrCreate();

        SQLContext sqlContext = sparkSession.sqlContext();
        //String path = "C:\\Formations\\data\\hbase\\";

        String path = args[0];

        Dataset<Row> dataset = sparkSession.read().option("header", true).csv(path);

        String hbaseCatalog = "{\r\n"
                + "\"table\":{\"namespace\":\"default\", \"name\":\"Contacts\", \"tableCoder\":\"PrimitiveType\"},\r\n"
                + "\"rowkey\":\"id\",\r\n"
                + "\"columns\":{\r\n"
                + "\"name\":{\"cf\":\"Personal\", \"col\":\"name\", \"type\":\"string\"},\r\n"
                + "\"phone\":{\"cf\":\"Personal\", \"col\":\"phone\", \"type\":\"string\"},\r\n"
                + "\"mobilePhone\":{\"cf\":\"Personal\", \"col\":\"mobilePhone\", \"type\":\"string\"},\r\n"
                + "\"fixeNumber\":{\"cf\":\"Office\", \"col\":\"fixeNumber\", \"type\":\"string\"},\r\n"
                + "\"adresse\":{\"cf\":\"Office\", \"col\":\"adresse\", \"type\":\"string\"}\r\n"
                + "}\r\n"
                + "}";

        Map<String, String> map = new HashMap();
        map.put(HBaseTableCatalog.tableCatalog(), hbaseCatalog);
        map.put(HBaseTableCatalog.newTable(), "1");

        dataset.write().options(map)
                .format("org.apache.spark.sql.execution.datasources.hbase")
                .save();
    }


    private static Dataset<org.apache.spark.sql.Row> withCatalog(String catalog,SQLContext sqlContext) {
        Map<String, String> map = new HashMap();
        map.put(HBaseTableCatalog.tableCatalog(), catalog);

        Dataset<Row> df = sqlContext.read().options(map)
                .format("org.apache.spark.sql.execution.datasources.hbase").load();
        df.show();
        return df;

// mvn -Dmaven.test.skip=true package
    }

    */
    }
}
