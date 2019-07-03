import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class newHadoopFile {


        private static Logger logger = Logger.getLogger(Collect_List.class);
        public static void main(String[] args) {

            SparkSession sparkSession = SparkSession
                    .builder()
                    .appName("Java Spark SQL basic example")
                    .config("spark.master", "local")
                    //.config( "spark.eventLog.enabled" , "true" )
                    .getOrCreate();
            JavaSparkContext sparkContext = new JavaSparkContext(sparkSession.sparkContext());

           //sparkContext.newAPIHadoopFile("", LongWritable.class, Text.class, Text.class, sparkContext.hadoopConfiguration()) ;

        }
}
