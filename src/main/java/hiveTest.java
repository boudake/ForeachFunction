import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.types.DataTypes.StringType;

public class hiveTest {


   // private static Logger logger = Logger.getLogger(Collect_List.class);

    public static void main(String[] args) {

        SparkSession sparkSession = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .config("spark.master", "local")
                //.config( "spark.eventLog.enabled" , "true" )
                .enableHiveSupport()
                .getOrCreate();

        List<String> list = Arrays.asList("Djim");
        list.add("Yace"); // Ajout d'un objet dans une list
        list.add("def");
        list.add("alll");
        list.add("def");
        JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());
        JavaRDD javaRDD = jsc.parallelize(list);

      Dataset dataset = sparkSession.createDataFrame(javaRDD,new StructType().add("fieldName", StringType, true));

      // dataset save soit en external ou internal table


        dataset.show();
    }
}