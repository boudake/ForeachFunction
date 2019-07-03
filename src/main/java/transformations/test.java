package transformations;

import org.apache.spark.HashPartitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class test {

    public static void main(String[] args) {

        SparkSession sparkSession = SparkSession
                .builder()
                .master("local")
                .getOrCreate();

        JavaSparkContext sc = new JavaSparkContext(sparkSession.sparkContext());

      List<Tuple2<String, Integer>> list = Arrays.asList(new Tuple2<>("Spark",78),new Tuple2<>("Hive",95),
              new Tuple2<>("spark",15),new Tuple2<>("HBase",25),new Tuple2<>("spark",39),new Tuple2<>("BigData",78),
              new Tuple2<>("spark",49));

       ;
       JavaPairRDD<String,Integer> javaPairRDD = sc.parallelize(list).mapToPair(pair -> new Tuple2<>(pair._1,pair._2()))

               ;





       // lookup
        javaPairRDD.lookup("spark").forEach(x->System.out.println(x));

        //countBykey

        System.out.println(javaPairRDD.countByKey());
    }

}
