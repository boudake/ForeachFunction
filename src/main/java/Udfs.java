import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import java.io.Serializable;

class Udfs implements Serializable {

    private CheckerImpl checker;

    Udfs() {
        checker = new CheckerImpl();
    }

    //CheckerImpl checker = new CheckerImpl();

    void udf(SparkSession sparkSession){

        sparkSession.udf().register("category", (String  age )-> checker.getCategory(age), DataTypes.StringType);
    }
}
