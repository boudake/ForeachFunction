package partitions;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

public class partitionTest {
    public static void main(String[] args) {

        SparkSession sparkSession = SparkSession
                .builder()
                .master("local")
                .getOrCreate();

        JavaSparkContext sc = new JavaSparkContext(sparkSession.sparkContext());

        String path = "C:\\Formations\\data\\partition\\" ;



        Dataset<Row> dataset =sparkSession.read().option("header", "true").csv(path);


        //System.out.println(dataset.rdd().getNumPartitions());

        try{
            final int read = System.in.read();
            System.out.println(read);
        }catch (Exception e){
           e.printStackTrace();
        }
        sparkSession.sparkContext().stop();
    }
    }

