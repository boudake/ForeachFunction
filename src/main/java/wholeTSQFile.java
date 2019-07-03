import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;

public class wholeTSQFile {

    public static void main(String[] args) {

        SparkSession sparkSession = SparkSession
                .builder()
                .appName("Java Spark ")
                .master("local")
                //.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                //.config("setOutputFormatClass", "SequenceFileOutputFormat.class")
                //.config( "spark.eventLog.enabled" , "true" )

                .getOrCreate();


        //config.setStrings("io.serializations", config.get("io.serializations"), MutationSerialization.class.getName(), ResultSerialization.class.getName());

        String path = "C:\\Formations\\data\\sequence";
        String in = "C:\\Formations\\data\\sequenceFile";
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkSession.sparkContext());

       // JavaPairRDD<Text,Text> javaPairRDD =javaSparkContext.parallelize(list).mapToPair(x -> new Tuple2<>(new Text(x),new Text("1")));


        //JavaPairRDD<Text,Text> javaPairRDD = javaSparkContext.wholeTextFiles(in).mapToPair(x -> new Tuple2<>(new Text(x._1),new Text(x._2)));
        //javaPairRDD.saveAsHadoopFile(path, Text.class, Text.class, SequenceFileOutputFormat.class);
         JavaRDD javaRDD = javaSparkContext.sequenceFile(path,Text.class, Text.class).flatMap(x -> {
            String fileName = x._1.toString() ;
            String [] lines = x._2.toString().split("\\r?\\n") ;
            return  Arrays.asList(lines).iterator();
        });

        System.out.println(javaRDD.collect())
        ;
        //javaSparkContext.wholeTextFiles("C:\\Formations\\data\\sequenceFile\\");


        try{
            final int read = System.in.read();
            System.out.println(read);
        }catch (Exception e){
        }
        sparkSession.sparkContext().stop();
    }
    }



