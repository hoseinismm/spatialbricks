package ir.smh.spatialbricks.core;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.api.java.JavaRDD;
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
import org.wololo.geojson.Feature;
import org.wololo.geojson.GeoJSONFactory;
import org.wololo.jts2geojson.GeoJSONReader;



import java.io.Serializable;
import java.util.*;

public class SpatialInputReader_version4 implements Serializable {

    private final SparkSession spark;

    private static final ThreadLocal<GeoJSONReader> GEOJSON_READER =
            ThreadLocal.withInitial(GeoJSONReader::new);

    public SpatialInputReader_version4(SparkSession spark) {
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
            Object value,
            DataType type) {

        if (value == null) {
            return null;
        }

        try {

            if (type.sameType(DataTypes.StringType)) {

                if (value instanceof Map || value instanceof List) {
                    return MAPPER.writeValueAsString(value);
                }

                return value.toString();
            }

            if (type.sameType(DataTypes.LongType)) {

                if (!(value instanceof Number)) {
                    return null;
                }

                return ((Number) value).longValue();
            }

            if (type.sameType(DataTypes.DoubleType)) {

                if (!(value instanceof Number)) {
                    return null;
                }

                return ((Number) value).doubleValue();
            }

            if (type.sameType(DataTypes.BooleanType)) {

                if (!(value instanceof Boolean)) {
                    return null;
                }

                return value;
            }

            return value.toString();

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

                Object obj = GeoJSONFactory.create(line);

                if (!(obj instanceof Feature feature)
                        || feature.getGeometry() == null) {
                    return null;
                }

                Geometry geometry =
                        GEOJSON_READER
                                .get()
                                .read(feature.getGeometry());

                Map<String, Object> properties =
                        feature.getProperties();

                List<Object> values =
                        new ArrayList<>(propertiesSchema.size() + 1);

                // geometry
                values.add(geometry);

                // properties
                for (StructField field : propertiesSchema.fields()) {

                    Object value =
                            properties == null
                                    ? null
                                    : properties.get(field.name());

                    values.add(
                            castValue(
                                    value,
                                    field.dataType()
                            )
                    );
                }

                return RowFactory.create(values.toArray());

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
}