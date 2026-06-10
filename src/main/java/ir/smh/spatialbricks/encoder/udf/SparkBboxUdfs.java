package ir.smh.spatialbricks.encoder.udf;

import ir.smh.spatialbricks.BucketManagerForBboxIndexing;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.Row;
import scala.collection.Seq;
public final class SparkBboxUdfs {

    private SparkBboxUdfs() {
    }

    private static final StructType BBOX_SCHEMA =
            DataTypes.createStructType(
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
                            )
                    });

    private static final StructType BUCKET_SCHEMA =
            DataTypes.createStructType(
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

    public static void registerCalculateBboxUdf(
            SparkSession spark) {

        spark.udf().register(
                "calculateBbox",
                (Seq<Row> parts) -> {

                    double[] bbox = calculateBounds(parts);

                    if (bbox == null) {
                        return RowFactory.create(
                                null,
                                null,
                                null,
                                null
                        );
                    }

                    return RowFactory.create(
                            bbox[0],
                            bbox[1],
                            bbox[2],
                            bbox[3]
                    );
                },
                BBOX_SCHEMA
        );
    }

    public static void registerFindBucketUdf(
            SparkSession spark,
            Broadcast<BucketManagerForBboxIndexing.Bucket> broadcastRootBuckets) {

        BucketManagerForBboxIndexing.Bucket root =
                broadcastRootBuckets.value();

        spark.udf().register(
                "findBucket",
                (Seq<Row> parts) -> {

                    double[] bbox = calculateBounds(parts);

                    if (bbox == null) {
                        return RowFactory.create(
                                null,
                                null,
                                null,
                                null,
                                null
                        );
                    }

                    BucketManagerForBboxIndexing.Bucket bucket =
                            findBucket(
                                    root,
                                    bbox[0],
                                    bbox[1],
                                    bbox[2],
                                    bbox[3]
                            );

                    return RowFactory.create(
                            bucket.xmin,
                            bucket.ymin,
                            bucket.xmax,
                            bucket.ymax,
                            bucket.code
                    );
                },
                BUCKET_SCHEMA
        );
    }

    private static BucketManagerForBboxIndexing.Bucket findBucket(
            BucketManagerForBboxIndexing.Bucket bucket,
            double minX,
            double minY,
            double maxX,
            double maxY) {

        while (bucket.hasChildren) {



                if (maxX <= bucket.xmid &&
                        minY >= bucket.ymid) {

                    bucket = bucket.topleft;

                } else if (minX >= bucket.xmid &&
                        minY >= bucket.ymid) {

                    bucket = bucket.topright;

                } else if (maxX <= bucket.xmid &&
                        maxY <= bucket.ymid) {

                    bucket = bucket.bottomleft;

                } else if (minX >= bucket.xmid &&
                        maxY <= bucket.ymid) {

                    bucket = bucket.bottomright;

                }
             else {
                    break;
                }
        }


        return bucket;
    }

    private static double[] calculateBounds(
            Seq<Row> parts) {

        if (parts == null || parts.isEmpty()) {
            return null;
        }

        double minX = Double.POSITIVE_INFINITY;
        double minY = Double.POSITIVE_INFINITY;
        double maxX = Double.NEGATIVE_INFINITY;
        double maxY = Double.NEGATIVE_INFINITY;

        for (int p = 0; p < parts.size(); p++) {

            Row part = parts.apply(p);

            @SuppressWarnings("unchecked")
            Seq<Row> coordinates =
                    (Seq<Row>) part.get(0);

            for (int i = 0; i < coordinates.size(); i++) {

                Row coordinate =
                        coordinates.apply(i);

                double x =
                        coordinate.getDouble(0);

                double y =
                        coordinate.getDouble(1);

                minX = Math.min(minX, x);
                minY = Math.min(minY, y);
                maxX = Math.max(maxX, x);
                maxY = Math.max(maxY, y);
            }
        }

        return new double[]{
                minX,
                minY,
                maxX,
                maxY
        };
    }
}

