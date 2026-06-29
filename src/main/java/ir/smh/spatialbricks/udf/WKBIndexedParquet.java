package ir.smh.spatialbricks.udf;

import ir.smh.spatialbricks.core.BucketManagerForBboxIndexing;
import ir.smh.spatialbricks.core.BucketManagerForBboxIndexing.Bucket;
import ir.smh.spatialbricks.decoder.WKBParquetDecoder;
import ir.smh.spatialbricks.encoder.GeometryResult;
import ir.smh.spatialbricks.encoder.converttogeometry.GeometryReader;
import ir.smh.spatialbricks.encoder.converttogeometry.WKBReaderAdapter;
import ir.smh.spatialbricks.encoder.converttogeometry.WKTReaderAdapter;
import ir.smh.spatialbricks.encoder.converttogeometry.geoJsonGeometricalAdapter;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT$;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.io.WKBWriter;

import java.io.Serializable;

import static org.apache.spark.sql.functions.*;

public class WKBIndexedParquet implements UDFRegistry<byte[],byte[]>, Serializable {

    public WKBIndexedParquet() {
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
                    DataTypes.createStructField("geom", DataTypes.BinaryType, false),
                    DataTypes.createStructField("bbox_partitioning", BUCKET_SCHEMA, true)
            });

    private static final WKBReader reader = new WKBReader();
    private static final WKBWriter wkbWriter = new WKBWriter();

    public byte[] parse(Geometry geometry) {
        return  ParseGeometryForWKB.parseGeometry(geometry);
    }

    // =========================================================
    // 1) GEOMETRY ENCODER UDF
    // =========================================================

    public DataType getGeometryType() {
        return GEOMETRY_TYPE;
    }

    public void registerGeometryUdf(SparkSession spark, GeometryReader adapter) {

        UDF1<Object, Row> udf = (Object input) -> {

            if (input == null) return null;

            try {

                byte[] geom;
                Geometry geometry;

                if (input instanceof byte[] && adapter instanceof WKBReaderAdapter) {
                    geom = (byte[])input;

                } else if (input instanceof String && adapter instanceof WKTReaderAdapter) {
                    geometry = ((WKTReaderAdapter) adapter).inputToGeometry((String) input);
                    geom =
                            ParseGeometryForWKB.parseGeometry(
                                    geometry
                            );

                } else  if (input instanceof Geometry && adapter instanceof geoJsonGeometricalAdapter) {
                    geometry = ((geoJsonGeometricalAdapter) adapter).inputToGeometry((Geometry) input);
                    geom =
                            ParseGeometryForWKB.parseGeometry(
                                    geometry
                            );

                } else {
                    throw new IllegalArgumentException("Unsupported input: " + input.getClass());
                }

                return geometryToRow(geom);

            } catch (Exception e) {
                System.err.println("Geometry UDF error: " + e.getMessage());
                return null;
            }
        };

        spark.udf().register("stringOrGeomToGeometry", udf, GEOMETRY_TYPE);
    }

    public Row geometryToRow(byte[] geom) {

        return new GenericRowWithSchema(
                new Object[]{
                        geom,
                        null
                },
                GEOMETRY_TYPE
        );
    }

    // =========================================================
    // 2) BBOX UDF
    // =========================================================

    public void registerBboxUdf(SparkSession spark) {

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

    public void registerBucketUdf(SparkSession spark,
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

    // =========================================================
    // DECODE UDF
    // =========================================================

    public void registerDecode(SparkSession spark) {

        spark.udf().register(
                "decodeGeometry",
                (Row geoRow) -> WKBParquetDecoder.geometryToJTS(geoRow),
                GeometryUDT$.MODULE$
        );
    }

    public void registerAddGeohash(SparkSession spark) {

        spark.udf().register(
                "addgeohash",
                (Row geoRow) -> {

                    try {
                        if (geoRow == null) return null;

                        Object g = geoRow.get(0);
                        if (g == null) return null;

                        byte[] geom = (byte[]) g;

                        Geometry geometry = reader.read(geom);

                        if (geometry == null || geometry.isEmpty()) {
                            return null;
                        }

                        Coordinate c = geometry.getCentroid().getCoordinate();

                        if (c == null) return null;

                        return GeometryResult.computeGeoHash(
                                c.getX(),
                                c.getY()
                        );

                    } catch (Exception e) {
                        return null; // مهم برای Spark stability
                    }
                },
                DataTypes.StringType
        );
    }



    // =========================================================
    // CORE LOGIC (shared)
    // =========================================================

    private double[] calculateBounds(Row geometry) {

        if (geometry == null) return null;

        try {
            Object wkb = geometry.get(0);
            if (wkb == null) return null;

            byte[] geom = (byte[]) wkb;

            Geometry g = reader.read(geom);

            if (g == null || g.isEmpty()) {
                return null;
            }

            Envelope env = g.getEnvelopeInternal();

            return new double[] {
                    env.getMinX(),
                    env.getMinY(),
                    env.getMaxX(),
                    env.getMaxY()
            };

        } catch (Exception e) {

            return null;
        }
    }

    private Bucket findBucket(
            Bucket bucket,
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

    public void registerCreatePointGeometry(SparkSession spark) {

        GeometryFactory geometryFactory = new GeometryFactory();
        WKBWriter writer = new WKBWriter();

        spark.udf().register(
                "createPointGeometry",
                (Double x, Double y) -> {

                    if (x == null || y == null) {
                        return null;
                    }

                    Point point = geometryFactory.createPoint(new Coordinate(x, y));
                    byte[] wkb = writer.write(point);

                    return RowFactory.create(
                            wkb,   // geom : Array<Binary>
                            null                  // bbox_partitioning
                    );
                },
                GEOMETRY_TYPE
        );
    }

    public Dataset<Row> addPointGeometryColumn(
            Dataset<Row> df,
            String xColumn,
            String yColumn,
            String geometryColumnName
    ) {
        return df.withColumn(
                geometryColumnName,
                callUDF(
                        "createPointGeometry",
                        col(xColumn),
                        col(yColumn)
                )
        );
    }
}