package groupBy;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Column ;
import org.apache.spark.sql.SparkSession;

public class test {

    public static void main(String[] args) {

        SparkSession sparkSession = SparkSession
                .builder()
                .master("local")
                .getOrCreate();

        String path = "C:\\Users\\ddiop\\Documents\\data\\data.csv" ;

        Dataset<Row> dataset = sparkSession.read().option("header", "true").csv(path);

       
    }
}
