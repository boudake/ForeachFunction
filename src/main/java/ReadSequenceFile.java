import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ReadSequenceFile {
    public static void main(String[] args) {

        SparkSession sparkSession = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .master("local")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                //.config( "spark.eventLog.enabled" , "true" )
                .getOrCreate();


        String path = args[0];
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkSession.sparkContext());

    }

}