import org.apache.log4j.Logger;
import org.apache.spark.sql.Row;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.Period;
import java.time.format.DateTimeFormatter;

class CheckerImpl implements Serializable {

    private static Logger logger = Logger.getLogger(CheckerImpl.class);


    boolean isMinor(Row row){
        return row.getAs("category").equals("Mineur");
    }

   String getCategory(String date){

       int age = getAge( date);
       if (age < 18){
           return "Mineur";
       }
       return "Majeur";
   }


    private static int getAge( String date){
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("MM/dd/yyyy");
        int age= 0;
        LocalDate now = LocalDate.now();
        try {
            LocalDate birthday = LocalDate.parse(date, formatter);
            age= Period.between(birthday, now).getYears();
        }catch (Exception e){
            logger.warn("ERROR found " + e.getMessage(), e);
            //System.exit(-1);
        }
        return age;
    }
}
