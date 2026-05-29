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
                .add("max_y", DataTypes.DoubleType, false);

        StructType geometryType = new StructType()
                .add("type", DataTypes.IntegerType, false)
                .add("parts", DataTypes.createArrayType(partType), false)
                .add(DataTypes.createStructField("bbox", bboxType, true))
                .add(DataTypes.createStructField("center", coordType, true))
                .add(DataTypes.createStructField("area", DataTypes.DoubleType, true))
                .add(DataTypes.createStructField("startpoint", coordType, true))
                .add(DataTypes.createStructField("endpoint", coordType, true))
                .add(DataTypes.createStructField("geohash_numeric", DataTypes.IntegerType, true))
                .add(DataTypes.createStructField("partition_number", bucketRangeType, true));

        UDF1<Object, Row> stringOrGeomToGeometry = (Object input) -> {
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
                        double x =  c.get("x");
                        double y =  c.get("y");
                        coordRows.add(new GenericRowWithSchema(new Object[]{x, y}, coordType));
                    }
                    partRows.add(new GenericRowWithSchema(new Object[]{coordRows}, partType));
                }

                List<Object> values = new ArrayList<>();
                values.add(type);            // 0: type
                values.add(partRows);        // 1: parts
                values.add(null);            // 2: bbox
                values.add(null);            // 3: center
                values.add(null);            // 4: area
                values.add(null);            // 5: startpoint
                values.add(null);            // 6: endpoint
                values.add(null);            // 7: geohash_numeric
                values.add(null);            // 8: partitionnumber


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
