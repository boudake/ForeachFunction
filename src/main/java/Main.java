import org.apache.log4j.Level;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import java.time.LocalDate;
import java.time.Period;
import java.time.format.DateTimeFormatter;
import org.apache.log4j.Logger;
import org.apache.spark.util.LongAccumulator;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;

public class Main {
    private static Logger logger = Logger.getLogger(Main.class);
    public static void main(String[] args) {


        SparkSession sparkSession = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .config("spark.master", "local")
                //.config( "spark.eventLog.enabled" , "true" )
                .getOrCreate();


        Logger.getLogger("org.apache.spark").setLevel(Level.ALL);

         Dataset dataset = sparkSession.read().option("header","true")
                 .option("inferSchema","true")
                 .option("delimiter", ",").csv("C:/Formations/data/data.csv") ;


        sparkSession.udf().register("category", (String  age )-> getCategory (age), DataTypes.StringType);

        sparkSession.udf().register("isMinor", (String  catgory )-> isMinor (catgory), DataTypes.BooleanType);


         SparkContext sc = sparkSession.sparkContext() ;


        sc.textFile("C:/Users/ddiop/Downloads/doc.txt",1) ;
        LongAccumulator accum = sc.longAccumulator();
       Dataset ds = dataset.withColumn("category", callUDF("category", col("date")));
       ds.foreach( (ForeachFunction<Row>) row  -> {
           if (isMinor (row)){
               accum.add(1);
           }

       });

       System.out.println("le nombre de ligne est: " + accum.value());
        ds.show();

       try{
           System.in.read();
       }catch (Exception e){
           logger.error("ERROR " + e.getMessage(), e);
       }
           sc.stop();
    }


    private static String getCategory (String date){

        int age = getAge( date);
        if (age < 18){
            return "Mineur";
        }
        return "Majeur";
    }

    private static boolean isMinor (String category){
        return category.equals("Mineur");
    }


    private static boolean isMinor (Row row){
        return row.getAs("category").equals("Mineur");
    }
    private static int getAge( String date){
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("MM/dd/yyyy");
        int age= 0;
        LocalDate now = LocalDate.now();
        try {
            LocalDate birthday = LocalDate.parse(date, formatter);
           age= Period.between(birthday, now).getYears();
        }catch (Exception e){
            logger.error("ERROR " + e.getMessage(), e);
           // System.exit(-1);
        }
        return age;
    }
}