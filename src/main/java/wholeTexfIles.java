import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class wholeTexfIles {

    public static void main(String[] args) {

        SparkSession sparkSession = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .master("local")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                //.config( "spark.eventLog.enabled" , "true" )
                .getOrCreate();

        String path = "C:/Formations/data/data.csv" ;


        JavaSparkContext sc = new JavaSparkContext(sparkSession.sparkContext());

       JavaPairRDD<String, String> javaPairRDD =  sc.wholeTextFiles(path);

        javaPairRDD.flatMap(x -> Arrays.asList(x._2.split("\\n"))
                .stream()
                .map(w -> new Tuple2<>(x._1, w))
                .iterator()).collect();

     JavaRDD<String > rdd = javaPairRDD.flatMap(x ->
        {
            ArrayList <String> list = new ArrayList<>();
            String fileName = x._1 ;
            String [] lines = x._2.split("\\r?\\n") ;
            String lineOutput ="";

            for (String line : lines){
               lineOutput = line + ","+ fileName;
               list.add(lineOutput);
            }
           return list.iterator();
        }).filter(x -> !x.startsWith("seq"));

     System.out.println(rdd.first());

        //System.out.println(javaPairRDD.flatMapValues(x -> Arrays.asList(x.split("\\W+"))).map(x -> x._2.split(",")).take(1));

    }

}