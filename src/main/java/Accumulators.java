import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;

import java.io.Serializable;

class Accumulators implements Serializable {
    private LongAccumulator accumMinor;
    private LongAccumulator accumMajor;
    private CheckerImpl checker;
    private static Logger logger = Logger.getLogger(Accumulators.class);


    Accumulators(SparkSession sparkSession) {
        checker = new CheckerImpl();
        accumMinor = sparkSession.sparkContext().longAccumulator();
        accumMajor = sparkSession.sparkContext().longAccumulator();
    }

    private void getNumberMinor(Dataset<Row> ds) {

        ds.foreach((ForeachFunction<Row>) row -> {
            if (checker.isMinor(row)) {
                accumMinor.add(1);
            }
        });
        logger.info("le nombre de Mineur est:" + accumMinor.value());
    }

    private void getNumberMajor(Dataset<Row> ds) {

        ds.foreach((ForeachFunction<Row>) row -> {
            if (!checker.isMinor(row)) {
                accumMajor .add(1);
            }
        });
        logger.info("le nombre de majeur est:" + accumMajor.value());
    }

    void displayAccumulators (Dataset<Row> ds){
        getNumberMajor( ds);
        getNumberMinor( ds);

    }
}
