import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

import static org.apache.spark.sql.functions.*;

public class Collect_List {
    private static Logger logger = Logger.getLogger(Collect_List.class);
    public static void main(String[] args) {

        SparkSession sparkSession = SparkSession.builder().
                master("local")
                .appName("spark session example")
                .getOrCreate();

        Udfs udfs = new Udfs();
        udfs.udf(sparkSession);
        Accumulators accumulators =new Accumulators(sparkSession);

        WindowSpec windowSpec = Window.partitionBy("category");
 Dataset<Row> dataset = sparkSession.read().option("header","true")
                 .option("inferSchema","true")
                 .option("delimiter", ",").csv("C:/Formations/data/data.csv") ;
        Column col = getCol ("category", "pick") ;
       Dataset<Row> ds = dataset.withColumn("category", callUDF("category", col("date")))
               .withColumn("colTest", col)
               //.withColumn("counte", count(col("pick").isNotNull()))
        ;


        ds.withColumn("list", collect_list("first").over(Window.partitionBy("category"))).show();

       ds.agg(sum(col("age")).alias("sum")).show();

        ds.show(1000);


       try{
           final int read = System.in.read();
           System.out.println(read);
       }catch (Exception e){
           logger.info("ERROR found " + e.getMessage(), e);
       }
        sparkSession.sparkContext().stop();
    }

   static Column getCol( String category, String pick) {
       return when(col(category).equalTo("Majeur").and(col(pick).equalTo("BLUE")),
               lit("M")).otherwise(lit("N"));
   }

}