package Hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.spark.api.java.JavaNewHadoopRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import java.util.LinkedList;
import java.util.List;



public class FileNameInSpark {

    public static void main(String[] args) {

        SparkSession sparkSession = SparkSession
                .builder()
                .appName("FileName")
                .master("local")
                .getOrCreate();
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkSession.sparkContext());

        JavaPairRDD<LongWritable, Text> javaPairRDD = javaSparkContext.newAPIHadoopFile("C:\\Formations\\data\\sequence",
                TextInputFormat.class, LongWritable.class, Text.class, new Configuration());

        JavaNewHadoopRDD<LongWritable, Text> hadoopRDD = (JavaNewHadoopRDD<LongWritable, Text>) javaPairRDD;

        JavaPairRDD<String, String> fileNameAndRecordsRDD =
                hadoopRDD.mapPartitionsWithInputSplit((x,y) -> {

                        FileSplit fileSplit = (FileSplit) x;
                        String fileLocation = fileSplit.getPath().toString();
                        List<Tuple2<String, String>> retList = new LinkedList<>();
                        while (y.hasNext()) {
                            String data = y.next()._2.toString();
                            retList.add(new Tuple2<>(fileLocation, data));
                        }
                        return retList.iterator();

                }, true).mapToPair(x -> new Tuple2<>(x._1, x._2) );

        /*Now print the records*/
        fileNameAndRecordsRDD.foreach(t -> System.out.println("File Name:" + t._1 + "##Record is:" + t._2));
    }
}