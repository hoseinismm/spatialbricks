package ir.smh.spatialbricks.encoder.udf;

import ir.smh.spatialbricks.encoder.GeometryReader;
import ir.smh.spatialbricks.encoder.udf.converttogeometry.WKBReaderAdapter;
import ir.smh.spatialbricks.encoder.udf.converttogeometry.WKTReaderAdapter;
import ir.smh.spatialbricks.encoder.udf.converttogeometry.geoJsonGeometricalAdapter;
import ir.smh.spatialbricks.encoder.udf.converttogeometry.geoJsonReaderAdapter;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.*;
import org.locationtech.jts.geom.Geometry;

import java.util.*;

public class UDFRegistry {

    public static void registerAll(SparkSession spark, GeometryReader adapter) {

        StructType coordType = new StructType()
                .add("x", DataTypes.DoubleType, false)
                .add("y", DataTypes.DoubleType, false);

        StructType bucketRangeType = new StructType()
                .add("floor", DataTypes.IntegerType, false)
                .add("ceiling", DataTypes.IntegerType, false);

        StructType partType = new StructType()
                .add("coordinates", DataTypes.createArrayType(coordType));

        StructType bboxType = new StructType()
                .add("min_x", DataTypes.DoubleType, false)
                .add("min_y", DataTypes.DoubleType, false)
                .add("max_x", DataTypes.DoubleType, false)
                .add("max_y", DataTypes.DoubleType, false)
                .add("region_code", DataTypes.IntegerType, false);

        StructType geometryType = new StructType()
                .add("type", DataTypes.IntegerType, false)
                .add("parts", DataTypes.createArrayType(partType), false)
                .add(DataTypes.createStructField("bbox_partitioning", bboxType, true))
                .add(DataTypes.createStructField("geohash_partitioning", bucketRangeType, true));

        UDF1<Object, Row> stringOrGeomToGeometry = (Object input) -> {

            if (input == null) {
                return null;
            }

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
                    throw new IllegalArgumentException("Unsupported input type: " + input.getClass());
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
                        coordRows.add(new GenericRowWithSchema(new Object[]{x, y}, coordType));
                    }
                    partRows.add(new GenericRowWithSchema(new Object[]{coordRows}, partType));
                }

                List<Object> values = new ArrayList<>();
                values.add(type);            // 0: type
                values.add(partRows);        // 1: parts
                values.add(null);            // 2: bboxpartitioning
                values.add(null);            // 3: geohashpartitioning


                return new GenericRowWithSchema(values.toArray(), geometryType);

            } catch (Exception e) {
                System.err.println("Error parsing geometry: " + e.getMessage());
                return null;
            }
        };

        // ثبت UDF
        spark.udf().register("stringOrGeomToGeometry", stringOrGeomToGeometry, geometryType);

    }
}
