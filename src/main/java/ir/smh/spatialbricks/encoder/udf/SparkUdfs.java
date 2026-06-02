package ir.smh.spatialbricks.encoder.udf;


import ir.smh.spatialbricks.encoder.GeometryResult;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;

public class SparkUdfs {

    public static void registerFindFloorAndCeilingUdf(
            SparkSession spark,
            Broadcast<int[]> broadcastBorders) {

        final int[] arr = broadcastBorders.value();

        StructType returnSchema = DataTypes.createStructType(
                new StructField[]{
                        DataTypes.createStructField(
                                "floor",
                                DataTypes.IntegerType,
                                true
                        ),
                        DataTypes.createStructField(
                                "ceiling",
                                DataTypes.IntegerType,
                                true
                        )
                });

        spark.udf().register(
                "findFloorAndCeiling",
                (Double x, Double y) -> {

                    if (x == null || y == null) {
                        return RowFactory.create(null, null);
                    }

                    int geo =
                            GeometryResult.computeGeohashNumeric(x,y);

                    int idx = Arrays.binarySearch(arr, geo);
                    int pos = (idx >= 0) ? idx : -(idx + 1);

                    Integer floor =
                            (idx >= 0)
                                    ? arr[idx]
                                    : (pos > 0 ? arr[pos - 1] : null);

                    int ceilIdx =
                            (idx >= 0)
                                    ? idx + 1
                                    : pos;

                    Integer ceiling =
                            (ceilIdx < arr.length)
                                    ? arr[ceilIdx]
                                    : null;

                    return RowFactory.create(
                            floor,
                            ceiling
                    );
                },
                returnSchema
        );
    }
}


