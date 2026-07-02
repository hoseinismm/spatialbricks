package ir.smh.spatialbricks.udf;

import ir.smh.spatialbricks.core.BucketManagerForBboxIndexing;
import ir.smh.spatialbricks.decoder.FlattenSpatialParquetDecoder;
import ir.smh.spatialbricks.encoder.converttogeometry.*;
import ir.smh.spatialbricks.encoder.GeometryResult;

import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT$;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.locationtech.jts.geom.Geometry;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.lit;

public class FlattenSpatialParquet implements UDFRegistry<Geometry,Map<String, Object>>, Serializable {

    SparkSession spark;

    public FlattenSpatialParquet(SparkSession spark) {
        this.spark = spark;
    }

    // =========================================================
    // SCHEMAS (shared)
    // =========================================================

    private static final StructType BBOX_SCHEMA =
            DataTypes.createStructType(new StructField[]{
                    DataTypes.createStructField("min_x", DataTypes.DoubleType, true),
                    DataTypes.createStructField("min_y", DataTypes.DoubleType, true),
                    DataTypes.createStructField("max_x", DataTypes.DoubleType, true),
                    DataTypes.createStructField("max_y", DataTypes.DoubleType, true)
            });

    private static final StructType BUCKET_SCHEMA =
            DataTypes.createStructType(new StructField[]{
                    DataTypes.createStructField("min_x", DataTypes.DoubleType, true),
                    DataTypes.createStructField("min_y", DataTypes.DoubleType, true),
                    DataTypes.createStructField("max_x", DataTypes.DoubleType, true),
                    DataTypes.createStructField("max_y", DataTypes.DoubleType, true),
                    DataTypes.createStructField("region_code", DataTypes.LongType, true)
            });

    private  static StructType GEOMETRY_TYPE =
            DataTypes.createStructType(new StructField[]{
                    DataTypes.createStructField("type", DataTypes.IntegerType, false),
                    DataTypes.createStructField("x", DataTypes.createArrayType(DataTypes.DoubleType), false),
                    DataTypes.createStructField("y", DataTypes.createArrayType(DataTypes.DoubleType), false),
                    DataTypes.createStructField("parts", DataTypes.createArrayType(DataTypes.IntegerType), false),
                    DataTypes.createStructField("bbox_partitioning", BUCKET_SCHEMA, true)
            });

    public Map<String, Object> parse(Geometry geometry) {
        return  ParseGeometryForFlatten.parseGeometry(geometry);
    }



    // =========================================================
    // 1) GEOMETRY ENCODER UDF
    // =========================================================

    public DataType getGeometryType() {
        return GEOMETRY_TYPE;
    }

    public void registerGeometryUdf( GeometryReader<?> adapter) {

        UDF1<Object, Row> udf = (Object input) -> {

            if (input == null) return null;

            try {

                Geometry geometry;

                if (input instanceof byte[] && adapter instanceof WKBReaderAdapter) {
                    geometry = ((WKBReaderAdapter) adapter).inputToGeometry((byte[]) input);

                } else if (input instanceof String && adapter instanceof WKTReaderAdapter) {
                    geometry = ((WKTReaderAdapter) adapter).inputToGeometry((String) input);

                } else  if (input instanceof Geometry && adapter instanceof GeoJsonGeometricalAdapter) {
                    geometry = ((GeoJsonGeometricalAdapter) adapter).inputToGeometry((Geometry) input);

                } else {
                    throw new IllegalArgumentException("Unsupported input: " + input.getClass());
                }

                return geometryToRow(geometry);

            } catch (Exception e) {
                System.err.println("Geometry UDF error: " + e.getMessage());
                return null;
            }
        };

        spark.udf().register("encodeGeometry", udf, GEOMETRY_TYPE);
    }

    public Row geometryToRow(Geometry geometry) {

        Map<String, Object> geom =
                ParseGeometryForFlatten.parseGeometry(
                        geometry
                );

        return new GenericRowWithSchema(
                new Object[]{
                        geom.get("type"),
                        geom.get("x"),
                        geom.get("y"),
                        geom.get("parts"),
                        null
                },
                GEOMETRY_TYPE
        );
    }

    // =========================================================
    // 2) BBOX UDF
    // =========================================================

    public void registerBboxUdf() {

        spark.udf().register(
                "calculateBbox",
                (Row geometry) -> {

                    double[] bbox = calculateBounds(geometry);

                    if (bbox == null) {
                        return RowFactory.create(null, null, null, null);
                    }

                    return RowFactory.create(bbox[0], bbox[1], bbox[2], bbox[3]);
                },
                BBOX_SCHEMA
        );
    }

    // =========================================================
    // 3) BUCKET UDF
    // =========================================================

    public void registerBucketUdf(Broadcast<BucketManagerForBboxIndexing.Bucket> broadcastRootBuckets) {
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


    // =========================================================
    // DECODE UDF
    // =========================================================

    public Geometry geometryToJTS(Row geoRow) {
        return FlattenSpatialParquetDecoder.geometryToJTS(geoRow);
    }

    public void registerDecode() {

        spark.udf().register(
                "decodeGeometry",
                (Row geoRow) -> FlattenSpatialParquetDecoder.geometryToJTS(geoRow),
                GeometryUDT$.MODULE$
        );
    }

    public void registerAddGeohash() {

        spark.udf().register(
                "addgeohash",
                (Row geoRow) -> {
                    List<Double> x = geoRow.getList(geoRow.fieldIndex("x"));
                    List<Double> y = geoRow.getList(geoRow.fieldIndex("y"));

                    return GeometryResult.computeGeoHash(
                            x.get(0),
                            y.get(0)
                    );
                },
                DataTypes.StringType   // اگر خروجی Geohash رشته است
        );
    }



    // =========================================================
    // CORE LOGIC (shared)
    // =========================================================

    private double[] calculateBounds(Row geometry) {

        if (geometry == null) return null;

        List<Double> x = geometry.getList(geometry.fieldIndex("x"));
        List<Double> y = geometry.getList(geometry.fieldIndex("y"));

        if (x == null || y == null || x.isEmpty()) return null;

        double minX = Double.POSITIVE_INFINITY;
        double minY = Double.POSITIVE_INFINITY;
        double maxX = Double.NEGATIVE_INFINITY;
        double maxY = Double.NEGATIVE_INFINITY;

        for (int i = 0; i < x.size(); i++) {

            double xi = x.get(i);
            double yi = y.get(i);

            if (xi < minX) minX = xi;
            if (yi < minY) minY = yi;
            if (xi > maxX) maxX = xi;
            if (yi > maxY) maxY = yi;
        }

        return new double[]{minX, minY, maxX, maxY};
    }

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

    public Dataset<Row> addPointGeometryColumn(
            Dataset<Row> df,
            String xColumn,
            String yColumn,
            String geometryColumnName
    ) {

        StructType bboxType = new StructType()
                .add("min_x", DataTypes.DoubleType, false)
                .add("min_y", DataTypes.DoubleType, false)
                .add("max_x", DataTypes.DoubleType, false)
                .add("max_y", DataTypes.DoubleType, false)
                .add("region_code", DataTypes.LongType, false);

        return df
                .withColumn(
                        geometryColumnName,
                        struct(
                                lit(1).alias("type"),

                                array(
                                        col(xColumn)
                                ).alias("x"),

                                array(
                                        col(yColumn)
                                ).alias("y"),

                                lit(null)
                                        .cast(DataTypes.createArrayType(DataTypes.IntegerType))
                                        .alias("parts"),

                                lit(null)
                                        .cast(bboxType)
                                        .alias("bbox_partitioning")
                        )
                )
                .select(geometryColumnName);

    }
}