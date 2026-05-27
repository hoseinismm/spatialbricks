package ir.smh.spatialbricks.encoder.udf;


import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;

public class SparkUdfs {

        public static void registerFindFloorUdf(
                SparkSession spark,
                Broadcast<int[]> broadcastBorders
        ) {

            final int[] arr = broadcastBorders.value();

            StructType returnSchema = DataTypes.createStructType(new StructField[]{
                    DataTypes.createStructField("floor", DataTypes.IntegerType, true),
                    DataTypes.createStructField("ceiling", DataTypes.IntegerType, true)
            });

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

            spark.udf().register("findFloorAndCeiling", (Integer value) -> {
                if (value == null) return RowFactory.create(null, null);

                int idx = Arrays.binarySearch(arr, value);
                int pos = (idx >= 0) ? idx : -(idx + 1);

                // محاسبه کف: اگر پیدا شد خودِ مقدار، اگر نشد عنصر قبلیِ مکان درج
                Integer floor = (idx >= 0) ? arr[idx] : (pos > 0 ? arr[pos - 1] : null);

                // محاسبه سقف: اگر پیدا شد عنصر بعدی، اگر نشد عنصرِ خودِ مکان درج
                int ceilIdx = (idx >= 0) ? idx + 1 : pos;
                Integer ceiling = (ceilIdx < arr.length) ? arr[ceilIdx] : null;

                return RowFactory.create(floor, ceiling);
            }, returnSchema);


        }
    }


