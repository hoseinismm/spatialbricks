package ir.smh.spatialbricks.core;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.sql.*;
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT$;
import org.apache.spark.sql.types.StructType;
import org.locationtech.jts.geom.*;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

public class SpatialInputReader implements Serializable {

    private final SparkSession spark;

    public SpatialInputReader(SparkSession spark) {
        this.spark = spark;
    }

    public Dataset<Row> read(String inputPath) {

        String path = inputPath.toLowerCase();

        if (path.endsWith(".json") || path.endsWith(".geojson")) {
            return spark.read().json(inputPath);
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