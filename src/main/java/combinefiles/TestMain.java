package combinefiles;

import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;


public class TestMain {

  public static void main(String[] args) {

    SparkSession sparkSession = SparkSession
            .builder()
            .appName("Java Spark")
            .master("local")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            //.config("setOutputFormatClass", "SequenceFileOutputFormat.class")
            //.config( "spark.eventLog.enabled" , "true" )

            .getOrCreate();

    JavaSparkContext sc = new JavaSparkContext(sparkSession.sparkContext());

  JavaPairRDD<FileLineWritable1, Text> javaPairRDD = sc.newAPIHadoopFile( "C:\\Formations\\data\\sequenceFile",CFInputFormat.class, FileLineWritable1.class, Text.class, sc.hadoopConfiguration())
         ;





   System.out.println(javaPairRDD.first());
  }
}
