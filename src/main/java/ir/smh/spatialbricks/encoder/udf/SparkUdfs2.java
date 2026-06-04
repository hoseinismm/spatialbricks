package ir.smh.spatialbricks.encoder.udf;

import ir.smh.spatialbricks.BucketManager2;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.Row;
import scala.collection.Seq;

public class SparkUdfs2 {

    public static void registerFindBucketUdf(
            SparkSession spark,
            Broadcast<BucketManager2.Bucket> broadcastBorders) {

        StructType returnSchema = DataTypes.createStructType(
                new StructField[]{
                        DataTypes.createStructField(
                                "min_x",
                                DataTypes.DoubleType,
                                true
                        ),
                        DataTypes.createStructField(
                                "min_y",
                                DataTypes.DoubleType,
                                true
                        ),
                        DataTypes.createStructField(
                                "max_x",
                                DataTypes.DoubleType,
                                true
                        ),
                        DataTypes.createStructField(
                                "max_y",
                                DataTypes.DoubleType,
                                true
                        ),
                        DataTypes.createStructField(
                                "region_code",
                                DataTypes.IntegerType,
                                true
                        )
                });

        BucketManager2.Bucket root = broadcastBorders.value();

        spark.udf().register(
                "findFloorAndCeiling",
                (Seq<Row> parts) -> {

                    BucketManager2.Bucket bucket = root;

                    if (parts == null || parts.isEmpty()) {
                        return RowFactory.create(
                                null,
                                null,
                                null,
                                null,
                                null
                        );
                    }

                    double min_x = Double.POSITIVE_INFINITY;
                    double min_y = Double.POSITIVE_INFINITY;
                    double max_x = Double.NEGATIVE_INFINITY;
                    double max_y = Double.NEGATIVE_INFINITY;

                    for (int p = 0; p < parts.size(); p++) {

                        Row part = parts.apply(p);

                        @SuppressWarnings("unchecked")
                        Seq<Row> coordinates =
                                (Seq<Row>) part.get(0);

                        for (int i = 0; i < coordinates.size(); i++) {

                            Row coord = coordinates.apply(i);

                            double x = coord.getDouble(0);
                            double y = coord.getDouble(1);

                            min_x = Math.min(min_x, x);
                            min_y = Math.min(min_y, y);
                            max_x = Math.max(max_x, x);
                            max_y = Math.max(max_y, y);
                        }
                    }


                    while (bucket.hasChildren) {
                        if (max_x <= bucket.xmid && min_y >= bucket.ymid) {
                            bucket = bucket.topleft;
                        } else if (min_x >= bucket.xmid && min_y >= bucket.ymid) {
                            bucket = bucket.topright;
                        } else if (max_x <= bucket.xmid && max_y <= bucket.ymid) {
                            bucket = bucket.bottomleft;
                        } else if (min_x >= bucket.xmid && max_y <= bucket.ymid) {
                            bucket = bucket.bottomright;
                        } else break;
                    }

                    return RowFactory.create(
                            bucket.xmin,bucket.ymin,bucket.xmax,bucket.ymax,bucket.code
                    );
                },
                returnSchema
        );
    }
}


