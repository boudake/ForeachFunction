package combinefiles;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FileLineWritable1 implements WritableComparable<FileLineWritable1> {

  public String fileName;

  public void readFields(DataInput in) throws IOException {
    this.fileName = Text.readString(in);
  }

  public void write(DataOutput out) throws IOException {

    Text.writeString(out, fileName);
  }

  public int compareTo(FileLineWritable1 that) {
    return this.fileName.compareTo(that.fileName);

  }

}