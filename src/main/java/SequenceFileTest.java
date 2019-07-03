import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer.Option;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;

import java.io.IOException;

public class SequenceFileTest {


     public static void main(String[] args) throws  IOException,
    InstantiationException, IllegalAccessException {
                // TODO Auto-generated method stub
                System.setProperty ("hadoop.home.dir", "C:\\Formations\\");
                Configuration conf = new Configuration();
                FileSystem fs = FileSystem.get(conf);
                Path inputFile = new Path("C:\\Formations\\data\\sequenceFile\\");
                Path outputFile = new Path("C:\\Formations\\data\\sequence");
                FSDataInputStream inputStream;
                Text key = new Text();
                Text value = new Text();

                Option fileOption = Writer.file(outputFile);
                Option keyClassOption = Writer.keyClass(key.getClass());
                Option valueClassOption = Writer.valueClass(value.getClass());
                Option compressOption = Writer.compression(SequenceFile.CompressionType.RECORD,
                        new GzipCodec());

                Writer writer = SequenceFile.createWriter( conf,
                        fileOption, keyClassOption, valueClassOption);
                FileStatus[] fStatus = fs.listStatus(inputFile);

                for (FileStatus fst : fStatus) {
                    String str = "";

                    System.out.println("Processing file : " + fst.getPath().getName() + " and the size is : " + fst.getPath().getName().length());
                    inputStream = fs.open(fst.getPath());
                    key.set(fst.getPath().getName());
                    while(inputStream.available()>0) {
                        str = str+inputStream.read();
                    }
                    value.set(str);
                    writer.append(key, value);

                }
                fs.close();
                IOUtils.closeStream(writer);
                System.out.println("SEQUENCE FILE CREATED SUCCESSFULLY........");
            }
        }