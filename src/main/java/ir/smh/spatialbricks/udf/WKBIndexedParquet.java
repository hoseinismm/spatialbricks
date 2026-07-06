package ir.smh.spatialbricks.udf;

import ir.smh.spatialbricks.core.BucketManager;
import ir.smh.spatialbricks.core.BucketManager.Bucket;
import ir.smh.spatialbricks.decoder.WKBParquetDecoder;
import ir.smh.spatialbricks.encoder.GeometryResult;
import ir.smh.spatialbricks.encoder.converttogeometry.GeometryReader;
import ir.smh.spatialbricks.encoder.converttogeometry.WKBReaderAdapter;
import ir.smh.spatialbricks.encoder.converttogeometry.WKTReaderAdapter;
import ir.smh.spatialbricks.encoder.converttogeometry.GeoJsonGeometricalAdapter;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT$;
import org.apache.spark.sql.types.*;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;

import java.io.Serializable;

import static ir.smh.spatialbricks.encoder.GeometryResult.lineFromMultiPoint;
import static org.apache.spark.sql.functions.*;

public class WKBIndexedParquet implements UDFRegistry<byte[],byte[]>, Serializable {
    SparkSession spark;

    public WKBIndexedParquet(SparkSession spark) {
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
                    DataTypes.createStructField("geom", DataTypes.BinaryType, false),
                    DataTypes.createStructField("bbox_partitioning", BUCKET_SCHEMA, true)
            });

    public byte[] parse(Geometry geometry) {
        return  ParseGeometryForWKB.parseGeometry(geometry);
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

                } else  if (input instanceof Geometry && adapter instanceof GeoJsonGeometricalAdapter) {
                    geometry = ((GeoJsonGeometricalAdapter) adapter).inputToGeometry((Geometry) input);
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

        spark.udf().register("encodeGeometry", udf, GEOMETRY_TYPE);
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

    public void registerBucketUdf(Broadcast<Bucket> broadcastRootBuckets) {
        Bucket root =
                broadcastRootBuckets.value();

        spark.udf().register(
                "findBucket",
                (Row geometry) -> {

                    double[] bbox = calculateBounds(geometry);

                    if (bbox == null) {
                        return RowFactory.create(null, null, null, null, null);
                    }

                    BucketManager.Bucket bucket =
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

    public Geometry geometryToJTS(Row geoRow) throws ParseException {
        return WKBParquetDecoder.geometryToJTS(geoRow);
    }

    public void registerDecode() {

        spark.udf().register(
                "decodeGeometry",
                (Row geoRow) -> WKBParquetDecoder.geometryToJTS(geoRow),
                GeometryUDT$.MODULE$
        );
    }

    public void registerAddGeohash() {

        spark.udf().register(
                "addgeohash",
                (Row geoRow) -> {

                    try {
                        if (geoRow == null) return null;

                        Object g = geoRow.get(0);
                        if (g == null) return null;

                        byte[] geom = (byte[]) g;

                        Geometry geometry = new WKBReader().read(geom);

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

    public void registerLineFromMultiPoint() {
        spark.udf().register(
                "multiPointToLine",
                (Row geoRow) -> {
                    Object g = geoRow.get(0);
                    if (g == null) {
                        return null;
                    }

                    try {
                        byte[] geom = (byte[]) g;
                        Geometry geometry = new WKBReader().read(geom);
                        return lineFromMultiPoint(geometry);
                    } catch (ParseException e) {
                        return null;
                    }
                },
                GeometryUDT$.MODULE$
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

            Geometry g = new WKBReader().read(geom);

            if (g.isEmpty()) {
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

    public void registerCreatePointGeometry() {

        GeometryFactory geometryFactory = new GeometryFactory();

        spark.udf().register(
                "createPointGeometry",
                (Double x, Double y) -> {

                    if (x == null || y == null) {
                        return null;
                    }

                    Point point = geometryFactory.createPoint(
                            new Coordinate(x, y)
                    );

                    return geometryToRow(
                            ParseGeometryForWKB.parseGeometry(point)
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
        registerCreatePointGeometry();
        return df.withColumn(
                geometryColumnName,
                callUDF(
                        "createPointGeometry",
                        col(xColumn),
                        col(yColumn)
                )).select(geometryColumnName);
    }
}