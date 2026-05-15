package ir.smh.spatialbricks.encoder.udf;


import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.*;

import java.util.Arrays;

public class SparkUdfs {

        public static void registerFindFloorUdf(
                SparkSession spark,
                Broadcast<int[]> broadcastBorders
        ) {

            final int[] arr = broadcastBorders.value();

            spark.udf().register(
                    "findFloor",
                    (Integer value) -> {

                        if (value == null) return null;

                        int idx = Arrays.binarySearch(arr, value);

                        if (idx >= 0) {
                            return arr[idx];
                        }

                        idx = -idx - 2;

                        if (idx < 0) {
                            return null;
                        }

                        return arr[idx];
                    },
                    DataTypes.IntegerType
            );
        }
    }


