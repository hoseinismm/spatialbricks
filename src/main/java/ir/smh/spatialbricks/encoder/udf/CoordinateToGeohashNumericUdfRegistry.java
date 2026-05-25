package ir.smh.spatialbricks.encoder.udf;


import ir.smh.spatialbricks.encoder.GeometryResult;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;



public class CoordinateToGeohashNumericUdfRegistry {


    public static void registerAll(SparkSession spark) {


        UDF2<Double, Double, Integer> myLogic = (x, y) -> {
            if (x == null || y == null) return null;
            return GeometryResult.computeGeohashNumeric(x , y);
        };

        spark.udf().register("CoordinateToGeohashNumeric", myLogic, DataTypes.IntegerType);
        }
    }

