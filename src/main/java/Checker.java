import org.apache.spark.sql.Row;

public interface Checker {
    String getCategory (String date);
    boolean isMinor(Row row);
}
