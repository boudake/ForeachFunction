package combineFileJson;

import combinefiles.FileLineWritable1;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;


public class TestMain {

  public static void main(String[] args) {

    SparkSession sparkSession = SparkSession
            .builder()
            .appName("Java Spark SQL basic example")
            .master("local")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            //.config("setOutputFormatClass", "SequenceFileOutputFormat.class")
            //.config( "spark.eventLog.enabled" , "true" )

            .getOrCreate();

    String path = "C:\\Formations\\data\\combinesFilesJson\\" ;

    JavaSparkContext sc = new JavaSparkContext(sparkSession.sparkContext());

 JavaRDD<String> javaRDD  = sc.newAPIHadoopFile( path, CFInputFormat.class, LongWritable.class, Text.class, sc.hadoopConfiguration())
    .map(x-> x._2.toString())
         ;


  Dataset<Row> dataset = sparkSession.read().json(javaRDD).filter(col("_corrupt_record").isNull());

    dataset.show();

    System.out.println(javaRDD.getNumPartitions()) ;
 // System.out.println(dataset.first());



  }
}
