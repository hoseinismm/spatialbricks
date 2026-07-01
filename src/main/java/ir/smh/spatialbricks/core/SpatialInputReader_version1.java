package ir.smh.spatialbricks.core;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT$;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.io.geojson.GeoJsonReader;

import java.io.Serializable;
import java.util.*;

public class SpatialInputReader_version1 implements Serializable {

    private final SparkSession spark;

    private static final ThreadLocal<GeoJsonReader> GEOJSON_READER =
            ThreadLocal.withInitial(
                    GeoJsonReader::new
            );

    public SpatialInputReader_version1(SparkSession spark) {
        this.spark = spark;
    }

    private StructType inferPropertiesSchema(
            String inputPath) throws Exception {

        List<String> samples =
                spark.read()
                        .textFile(inputPath)
                        .takeAsList(100);

        ObjectMapper mapper = new ObjectMapper();

        Map<String, DataType> columns =
                new LinkedHashMap<>();

        for (String line : samples) {

            JsonNode root = mapper.readTree(line);

            JsonNode properties = root.get("properties");

            if (properties == null || !properties.isObject()) {
                continue;
            }

            Iterator<Map.Entry<String, JsonNode>> fields =
                    properties.fields();

            while (fields.hasNext()) {

                Map.Entry<String, JsonNode> field =
                        fields.next();

                String name = field.getKey();
                JsonNode value = field.getValue();

                if (value == null || value.isNull()) {
                    continue;
                }

                DataType detectedType;

                if (value.isTextual()) {
                    detectedType = DataTypes.StringType;
                }
                else if (value.isIntegralNumber()) {
                    detectedType = DataTypes.LongType;
                }
                else if (value.isFloatingPointNumber()) {
                    detectedType = DataTypes.DoubleType;
                }
                else if (value.isBoolean()) {
                    detectedType = DataTypes.BooleanType;
                }
                else {
                    detectedType = DataTypes.StringType;
                }

                DataType existing = columns.get(name);

                if (existing == null) {
                    columns.put(name, detectedType);
                }
                else if (!existing.sameType(detectedType)) {
                    columns.put(name, DataTypes.StringType);
                }
            }
        }

        StructType schema = new StructType();

        for (Map.Entry<String, DataType> col : columns.entrySet()) {

            schema = schema.add(
                    col.getKey(),
                    col.getValue(),
                    true
            );
        }

        return schema;
    }

    private StructType addGeometryColumn(
            StructType propertiesSchema) {

        StructType schema = new StructType()
                .add(
                        "geometry",
                        GeometryUDT$.MODULE$,
                        true
                );

        for (StructField field : propertiesSchema.fields()) {
            schema = schema.add(field);
        }

        return schema;
    }

    private Object castValue(
            JsonNode node,
            DataType type) {

        if (node == null || node.isNull()) {
            return null;
        }

        try {

            if (type.sameType(DataTypes.StringType)) {

                if (node.isContainerNode()) {
                    return node.toString();
                }

                return node.asText();
            }

            if (type.sameType(DataTypes.LongType)) {

                if (!node.isNumber()) {
                    return null;
                }

                return node.asLong();
            }

            if (type.sameType(DataTypes.DoubleType)) {

                if (!node.isNumber()) {
                    return null;
                }

                return node.asDouble();
            }

            if (type.sameType(DataTypes.BooleanType)) {

                if (!node.isBoolean()) {
                    return null;
                }

                return node.asBoolean();
            }

            return node.toString();

        } catch (Exception e) {

            return null;
        }
    }

    private static final ObjectMapper MAPPER =
            new ObjectMapper();

    private JavaRDD<Row> buildRows(
            JavaRDD<String> lines,
            StructType propertiesSchema) {

        return lines.map(line -> {

            try {

                JsonNode root =
                        MAPPER.readTree(line);

                JsonNode geometryNode =
                        root.get("geometry");

                if (geometryNode == null || geometryNode.isNull()) {
                    return null;
                }

                JsonNode properties =
                        root.get("properties");

                List<Object> values =
                        new ArrayList<>();

                // geometry
                Geometry geometry =
                        GEOJSON_READER
                                .get()
                                .read(
                                        geometryNode.toString()
                                );

                values.add(geometry);

                // properties
                for (StructField field :
                        propertiesSchema.fields()) {

                    JsonNode value =
                            properties == null
                                    ? null
                                    : properties.get(
                                    field.name()
                            );

                    values.add(
                            castValue(
                                    value,
                                    field.dataType()
                            )
                    );
                }

                return RowFactory.create(
                        values.toArray()
                );

            } catch (Exception e) {

                System.err.println(
                        "Error parsing feature: "
                                + e.getMessage()
                );

                System.err.println(
                        "Line: "
                                + line
                );

                return null;
            }

        }).filter(Objects::nonNull);
    }

    public Dataset<Row> read(String inputPath) throws Exception {

        String path = inputPath.toLowerCase();

        if (path.endsWith(".json") || path.endsWith(".geojson")) {



            StructType propertiesSchema =
                    inferPropertiesSchema(
                            inputPath
                    );

            StructType finalSchema =
                    addGeometryColumn(
                            propertiesSchema
                    );

            JavaRDD<String> lines =
                    spark.read()
                            .textFile(inputPath)
                            .javaRDD();

            JavaRDD<Row> rows =
                    buildRows(
                            lines,
                            propertiesSchema
                    );

            Dataset<Row> df =
                    spark.createDataFrame(
                            rows,
                            finalSchema
                    );

            return df;
        }

        if (path.endsWith(".parquet")) {
            return spark.read().parquet(inputPath);
        }

        if (path.endsWith(".csv")) {
            return spark.read().csv(inputPath);
        }

        throw new IllegalArgumentException("Unsupported file format: " + inputPath);
    }

    private Dataset<Row> readGeoJson(String inputPath, JavaSparkContext jsc) {

        JavaRDD<String> lines = jsc.textFile(inputPath);

        JavaRDD<Row> rows = lines.map(line -> {

            try {
                ObjectMapper mapper = new ObjectMapper();

                JsonNode root = mapper.readTree(line);

                JsonNode geometryNode = root.get("geometry");
                if (geometryNode == null || geometryNode.isNull()) {
                    return null;
                }

                String geometryType = geometryNode.get("type").asText();

                Object coordinates = mapper.convertValue(
                        geometryNode.get("coordinates"),
                        Object.class
                );

                Geometry geometry = buildGeometryFromGeoJSON(geometryType, coordinates);

                return RowFactory.create(geometry);

            } catch (Exception e) {
                return null;
            }

        }).filter(Objects::nonNull);

        StructType schema = new StructType()
                .add("geometry", GeometryUDT$.MODULE$);

        return spark.createDataFrame(rows, schema);
    }


    @SuppressWarnings("unchecked")
    public Geometry buildGeometryFromGeoJSON(String type, Object coordsObj) {

        GeometryFactory gf = new GeometryFactory();

        switch (type) {

            // ---------------- POINT ----------------
            case "Point": {
                List<Double> c = (List<Double>) coordsObj;
                return gf.createPoint(new Coordinate(c.get(0), c.get(1)));
            }

            // ---------------- LINESTRING ----------------
            case "LineString": {
                List<List<Double>> c = (List<List<Double>>) coordsObj;

                Coordinate[] coords = c.stream()
                        .map(p -> new Coordinate(p.get(0), p.get(1)))
                        .toArray(Coordinate[]::new);

                return gf.createLineString(coords);
            }

            // ---------------- POLYGON ----------------
            case "Polygon": {
                List<List<List<Double>>> rings = (List<List<List<Double>>>) coordsObj;

                LinearRing shell = gf.createLinearRing(
                        toCoords(rings.get(0))
                );

                LinearRing[] holes = new LinearRing[Math.max(0, rings.size() - 1)];

                for (int i = 1; i < rings.size(); i++) {
                    holes[i - 1] = gf.createLinearRing(
                            toCoords(rings.get(i))
                    );
                }

                return gf.createPolygon(shell, holes);
            }

            // ---------------- MULTIPOINT ----------------
            case "MultiPoint": {
                List<List<Double>> c = (List<List<Double>>) coordsObj;

                Point[] points = c.stream()
                        .map(p -> gf.createPoint(new Coordinate(p.get(0), p.get(1))))
                        .toArray(Point[]::new);

                return gf.createMultiPoint(points);
            }

            // ---------------- MULTILINESTRING ----------------
            case "MultiLineString": {
                List<List<List<Double>>> lines =
                        (List<List<List<Double>>>) coordsObj;

                LineString[] arr = lines.stream()
                        .map(line -> gf.createLineString(toCoords(line)))
                        .toArray(LineString[]::new);

                return gf.createMultiLineString(arr);
            }

            // ---------------- MULTIPOLYGON ----------------
            case "MultiPolygon": {
                List<List<List<List<Double>>>> polys =
                        (List<List<List<List<Double>>>>) coordsObj;

                Polygon[] arr = polys.stream()
                        .map(p -> {
                            LinearRing shell = gf.createLinearRing(toCoords(p.get(0)));

                            LinearRing[] holes = new LinearRing[Math.max(0, p.size() - 1)];
                            for (int i = 1; i < p.size(); i++) {
                                holes[i - 1] = gf.createLinearRing(toCoords(p.get(i)));
                            }

                            return gf.createPolygon(shell, holes);
                        })
                        .toArray(Polygon[]::new);

                return gf.createMultiPolygon(arr);
            }

            default:
                throw new IllegalArgumentException("Unsupported type: " + type);
        }
    }
    private Coordinate[] toCoords(List<List<Double>> list) {
        return list.stream()
                .map(p -> new Coordinate(p.get(0), p.get(1)))
                .toArray(Coordinate[]::new);
    }
}