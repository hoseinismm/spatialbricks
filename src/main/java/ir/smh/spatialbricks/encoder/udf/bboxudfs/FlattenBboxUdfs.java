package ir.smh.spatialbricks.encoder.udf.bboxudfs;

import ir.smh.spatialbricks.BucketManagerForBboxIndexing;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;

public final class FlattenBboxUdfs implements SparkBboxUdfs, Serializable {

    private FlattenBboxUdfs() {}

    private final StructType BBOX_SCHEMA =
            DataTypes.createStructType(new StructField[]{
                    DataTypes.createStructField("min_x", DataTypes.DoubleType, true),
                    DataTypes.createStructField("min_y", DataTypes.DoubleType, true),
                    DataTypes.createStructField("max_x", DataTypes.DoubleType, true),
                    DataTypes.createStructField("max_y", DataTypes.DoubleType, true)
            });

    private final StructType BUCKET_SCHEMA =
            DataTypes.createStructType(new StructField[]{
                    DataTypes.createStructField("min_x", DataTypes.DoubleType, true),
                    DataTypes.createStructField("min_y", DataTypes.DoubleType, true),
                    DataTypes.createStructField("max_x", DataTypes.DoubleType, true),
                    DataTypes.createStructField("max_y", DataTypes.DoubleType, true),
                    DataTypes.createStructField("region_code", DataTypes.LongType, true)
            });

    // ---------------- REGISTER BBOX ----------------

    public void registerCalculateBboxUdf(SparkSession spark) {

        spark.udf().register(
                "calculateBbox",
                (Row geometry) -> {

                    double[] bbox = calculateBounds(geometry);

                    if (bbox == null) {
                        return RowFactory.create(null, null, null, null);
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

    // ---------------- REGISTER BUCKET ----------------

    public void registerFindBucketUdf(
            SparkSession spark,
            Broadcast<BucketManagerForBboxIndexing.Bucket> broadcastRootBuckets) {

        BucketManagerForBboxIndexing.Bucket root =
                broadcastRootBuckets.value();

        spark.udf().register(
                "findBucket",
                (Row geometry) -> {

                    double[] bbox = calculateBounds(geometry);

                    if (bbox == null) {
                        return RowFactory.create(null, null, null, null, null);
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

    // ---------------- BUCKET TRAVERSAL ----------------

    private BucketManagerForBboxIndexing.Bucket findBucket(
            BucketManagerForBboxIndexing.Bucket bucket,
            double minX,
            double minY,
            double maxX,
            double maxY) {

        while (bucket.hasChildren) {

            if (maxX <= bucket.xmid && minY >= bucket.ymid) {
                bucket = bucket.topleft;

            } else if (minX >= bucket.xmid && minY >= bucket.ymid) {
                bucket = bucket.topright;

            } else if (maxX <= bucket.xmid && maxY <= bucket.ymid) {
                bucket = bucket.bottomleft;

            } else if (minX >= bucket.xmid && maxY <= bucket.ymid) {
                bucket = bucket.bottomright;

            } else {
                break;
            }
        }

        return bucket;
    }

    // ---------------- BBOX CALCULATION (NEW FORMAT) ----------------

    private double[] calculateBounds(Row geometry) {

        if (geometry == null) {
            return null;
        }

        double[] x = geometry.getAs("x");
        double[] y = geometry.getAs("y");

        if (x == null || y == null || x.length == 0) {
            return null;
        }

        double minX = Double.POSITIVE_INFINITY;
        double minY = Double.POSITIVE_INFINITY;
        double maxX = Double.NEGATIVE_INFINITY;
        double maxY = Double.NEGATIVE_INFINITY;

        // ساده‌ترین حالت: scan کل نقاط (fast + safe)
        for (int i = 0; i < x.length; i++) {
            double xi = x[i];
            double yi = y[i];

            if (xi < minX) minX = xi;
            if (yi < minY) minY = yi;
            if (xi > maxX) maxX = xi;
            if (yi > maxY) maxY = yi;
        }

        return new double[]{minX, minY, maxX, maxY};
    }
}