package ir.smh.spatialbricks.encoder.udf;

import ir.smh.spatialbricks.encoder.GeometryResult;
import ir.smh.spatialbricks.encoder.GeometryOptions;
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

        StructType partType = new StructType()
                .add("coordinates", DataTypes.createArrayType(coordType));

        StructType bboxType = new StructType()
                .add("min_x", DataTypes.DoubleType)
                .add("min_y", DataTypes.DoubleType)
                .add("max_x", DataTypes.DoubleType)
                .add("max_y", DataTypes.DoubleType);

        StructType geometryType = new StructType()
                .add("type", DataTypes.IntegerType, false)
                .add("parts", DataTypes.createArrayType(partType), false)
                .add(DataTypes.createStructField("bbox", bboxType, true))
                .add(DataTypes.createStructField("center", coordType, true))
                .add(DataTypes.createStructField("area", DataTypes.DoubleType, true))
                .add(DataTypes.createStructField("startpoint", coordType, true))
                .add(DataTypes.createStructField("endpoint", coordType, true))
                .add(DataTypes.createStructField("geohash", DataTypes.StringType, true));





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



            GeometryResult result = ParseGeometry.parseGeometry(geometry);
                Map<String, Object> geom = result.geomMap;
                int type = (int) geom.get("type");
                @SuppressWarnings("unchecked")
                List<List<Map<String, Object>>> partsList = (List<List<Map<String, Object>>>) geom.get("part");

                List<Row> partRows = new ArrayList<>();
                for (List<Map<String, Object>> part : partsList) {
                    List<Row> coordRows = new ArrayList<>();
                    for (Map<String, Object> c : part) {
                        double x = (double) c.get("x");
                        double y = (double) c.get("y");
                        coordRows.add(new GenericRowWithSchema(new Object[]{x, y}, coordType));
                    }
                    partRows.add(new GenericRowWithSchema(new Object[]{coordRows}, partType));
                }

                List<Object> values = new ArrayList<>();
                values.add(type);
                values.add(partRows);

                if (options.has("bbox"))
                    values.add(result.computeBBoxRow(bboxType));
                if (options.has("center"))
                    values.add(result.computeCenterRow(centerType));
                if (options.has("area"))
                    values.add(result.computeArea());
                if (options.has("startpoint"))
                    values.add(result.computeStartPointRow(coordType));
                if (options.has("endpoint"))
                    values.add(result.computeEndPointRow(coordType));
                if (options.has("geohash"))
                    values.add(result.computeGeoHash());

                return new GenericRowWithSchema(values.toArray(), geometryType);

            } catch (Exception e) {
                // خطا رخ داد → مقدار null برگردان
                e.printStackTrace();
                return null;
            }
        };

        // ثبت UDF
        spark.udf().register("stringOrGeomToGeometry", stringOrGeomToGeometry, geometryType);

    }
}
