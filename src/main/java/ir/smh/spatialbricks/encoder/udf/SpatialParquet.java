package ir.smh.spatialbricks.encoder.udf;

import ir.smh.spatialbricks.BucketManagerForBboxIndexing;
import ir.smh.spatialbricks.decoder.SpatialParquetDecoder;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT$;
import org.apache.spark.sql.types.*;
import org.locationtech.jts.geom.Geometry;

import java.io.Serializable;
import java.util.*;

import ir.smh.spatialbricks.encoder.GeometryReader;
import ir.smh.spatialbricks.encoder.udf.converttogeometry.*;

public class SpatialParquet implements UDFRegistry, Serializable {

    public SpatialParquet() {
    }

    // =========================================================
    // SCHEMAS
    // =========================================================

    private static final StructType COORD_TYPE =
            DataTypes.createStructType(new StructField[]{
                    DataTypes.createStructField("x", DataTypes.DoubleType, false),
                    DataTypes.createStructField("y", DataTypes.DoubleType, false)
            });

    private static final StructType PART_TYPE =
            DataTypes.createStructType(new StructField[]{
                    DataTypes.createStructField("coordinates",
                            DataTypes.createArrayType(COORD_TYPE), false)
            });

    private static final StructType BBOX_TYPE =
            DataTypes.createStructType(new StructField[]{
                    DataTypes.createStructField("min_x", DataTypes.DoubleType, true),
                    DataTypes.createStructField("min_y", DataTypes.DoubleType, true),
                    DataTypes.createStructField("max_x", DataTypes.DoubleType, true),
                    DataTypes.createStructField("max_y", DataTypes.DoubleType, true),
                    DataTypes.createStructField("region_code", DataTypes.LongType, true)
            });

    private static final StructType GEOMETRY_TYPE =
            DataTypes.createStructType(new StructField[]{
                    DataTypes.createStructField("type", DataTypes.IntegerType, false),
                    DataTypes.createStructField("parts",
                            DataTypes.createArrayType(PART_TYPE), false),
                    DataTypes.createStructField("bbox_partitioning", BBOX_TYPE, true)
            });



    // =========================================================
    // 1) GEOMETRY ENCODER (FLAT PARTS STRUCTURE)
    // =========================================================

    public void registerGeometryUdf(SparkSession spark, GeometryReader adapter) {

        UDF1<Object, Row> udf = (Object input) -> {

            if (input == null) return null;

            try {

                Geometry geometry;

                if (input instanceof byte[] && adapter instanceof WKBReaderAdapter) {
                    geometry = ((WKBReaderAdapter) adapter).inputToGeometry((byte[]) input);

                } else if (input instanceof String && adapter instanceof WKTReaderAdapter) {
                    geometry = ((WKTReaderAdapter) adapter).inputToGeometry((String) input);

                } else if (input instanceof Geometry && adapter instanceof geoJsonGeometricalAdapter) {
                    geometry = ((geoJsonGeometricalAdapter) adapter).inputToGeometry((Geometry) input);

                } else if (input instanceof String && adapter instanceof geoJsonReaderAdapter) {
                    geometry = ((geoJsonReaderAdapter) adapter).inputToGeometry((String) input);

                } else {
                    throw new IllegalArgumentException("Unsupported input: " + input.getClass());
                }
                Map<String, Object> geom = ParseGeometry.parseGeometry(geometry);

                int type = (int) geom.get("type");
                @SuppressWarnings("unchecked")
                List<List<Map<String, Double>>> partsList = (List<List<Map<String, Double>>>) geom.get("parts");

                List<Row> partRows = new ArrayList<>();
                for (List<Map<String, Double>> part : partsList) {
                    List<Row> coordRows = new ArrayList<>();
                    for (Map<String, Double> c : part) {
                        double x = c.get("x");
                        double y = c.get("y");
                        coordRows.add(new GenericRowWithSchema(new Object[]{x, y}, COORD_TYPE));
                    }
                    partRows.add(new GenericRowWithSchema(new Object[]{coordRows}, PART_TYPE));
                }

                List<Object> values = new ArrayList<>();
                values.add(type);            // 0: type
                values.add(partRows);        // 1: parts
                values.add(null);            // 2: bboxpartitioning

                return new GenericRowWithSchema(values.toArray(), GEOMETRY_TYPE);

            } catch (Exception e) {
                System.err.println("Error parsing geometry: " + e.getMessage());
                return null;
            }
        };

        // ثبت UDF
        spark.udf().register("stringOrGeomToGeometry", udf, GEOMETRY_TYPE);

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
                DataTypes.createStructType(new StructField[]{
                        DataTypes.createStructField("min_x", DataTypes.DoubleType, true),
                        DataTypes.createStructField("min_y", DataTypes.DoubleType, true),
                        DataTypes.createStructField("max_x", DataTypes.DoubleType, true),
                        DataTypes.createStructField("max_y", DataTypes.DoubleType, true)
                })
        );
    }

    // =========================================================
    // 3) BUCKET UDF
    // =========================================================

    public void registerBucketUdf(SparkSession spark,
                                  Broadcast<BucketManagerForBboxIndexing.Bucket> broadcast) {

        BucketManagerForBboxIndexing.Bucket root = broadcast.value();

        spark.udf().register(
                "findBucket",
                (Row geometry) -> {

                    double[] bbox = calculateBounds(geometry);

                    if (bbox == null) {
                        return RowFactory.create(null, null, null, null, null);
                    }

                    BucketManagerForBboxIndexing.Bucket bucket =
                            findBucket(root, bbox[0], bbox[1], bbox[2], bbox[3]);

                    return RowFactory.create(
                            bucket.xmin,
                            bucket.ymin,
                            bucket.xmax,
                            bucket.ymax,
                            bucket.code
                    );
                },
                BBOX_TYPE
        );
    }

    // =========================================================
    // DECODE UDF
    // =========================================================

    public void registerDecode(SparkSession spark) {

        spark.udf().register(
                "decodeGeometry",
                (Row geoRow) -> SpatialParquetDecoder.geometryToJTS(geoRow),
                GeometryUDT$.MODULE$
        );
    }

    // =========================================================
    // CORE LOGIC
    // =========================================================


    private double[] calculateBounds(Row geometry) {

        if (geometry == null) return null;

        List<Row> parts = geometry.getList(1);
        if (parts == null || parts.isEmpty()) return null;

        double minX = Double.POSITIVE_INFINITY;
        double minY = Double.POSITIVE_INFINITY;
        double maxX = Double.NEGATIVE_INFINITY;
        double maxY = Double.NEGATIVE_INFINITY;

        for (Row part : parts) {

            List<Row> coords = part.getList(0);

            for (Row c : coords) {
                double x = c.getDouble(0);
                double y = c.getDouble(1);

                if (x < minX) minX = x;
                if (y < minY) minY = y;
                if (x > maxX) maxX = x;
                if (y > maxY) maxY = y;
            }
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

}