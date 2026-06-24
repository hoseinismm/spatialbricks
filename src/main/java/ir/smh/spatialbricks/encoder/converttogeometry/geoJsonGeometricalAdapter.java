package ir.smh.spatialbricks.encoder.converttogeometry;

import org.apache.spark.sql.Row;
import org.locationtech.jts.geom.*;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class geoJsonGeometricalAdapter implements GeometryReader<Row>, Serializable {

    @Override
    public Geometry inputToGeometry(Row row) {

        String type = row.getAs("type");
        Object coordinates = row.getAs("coordinates");

        return buildGeometryFromGeoJSON(type, coordinates);
    }

    private List<Object> asList(Object obj) {

        if (obj instanceof List<?>) {
            return new ArrayList<>((List<?>) obj);
        }

        if (obj instanceof scala.collection.Iterable<?>) {

            List<Object> result = new ArrayList<>();

            scala.jdk.javaapi.CollectionConverters
                    .asJava((scala.collection.Iterable<?>) obj)
                    .forEach(result::add);

            return result;
        }

        throw new IllegalArgumentException(
                "Unsupported collection type: " + obj.getClass()
        );
    }

    @SuppressWarnings("unchecked")
    public Geometry buildGeometryFromGeoJSON(
            String type,
            Object coordsObj) {

        GeometryFactory gf = new GeometryFactory();

        switch (type) {

            // ---------- Point ----------
            case "Point": {

                List<?> c = asList(coordsObj);

                return gf.createPoint(
                        new Coordinate(
                                toDouble(c.get(0)),
                                toDouble(c.get(1))
                        )
                );
            }

            // ---------- LineString ----------
            case "LineString": {

                List<?> points = asList(coordsObj);

                Coordinate[] coords = points.stream()
                        .map(p -> {
                            List<?> pt = asList(p);

                            return new Coordinate(
                                    toDouble(pt.get(0)),
                                    toDouble(pt.get(1))
                            );
                        })
                        .toArray(Coordinate[]::new);

                return gf.createLineString(coords);
            }

            // ---------- Polygon ----------
            case "Polygon": {

                List<?> rings = asList(coordsObj);

                LinearRing shell =
                        gf.createLinearRing(
                                toCoords(asList(rings.get(0)))
                        );

                LinearRing[] holes =
                        new LinearRing[Math.max(0, rings.size() - 1)];

                for (int i = 1; i < rings.size(); i++) {
                    holes[i - 1] =
                            gf.createLinearRing(
                                    toCoords(asList(rings.get(i)))
                            );
                }

                return gf.createPolygon(shell, holes);
            }

            // ---------- MultiPoint ----------
            case "MultiPoint": {

                List<?> points = asList(coordsObj);

                Point[] arr = points.stream()
                        .map(p -> {
                            List<?> pt = asList(p);

                            return gf.createPoint(
                                    new Coordinate(
                                            toDouble(pt.get(0)),
                                            toDouble(pt.get(1))
                                    )
                            );
                        })
                        .toArray(Point[]::new);

                return gf.createMultiPoint(arr);
            }

            // ---------- MultiLineString ----------
            case "MultiLineString": {

                List<?> lines = asList(coordsObj);

                LineString[] arr = lines.stream()
                        .map(line ->
                                gf.createLineString(
                                        toCoords(asList(line))
                                )
                        )
                        .toArray(LineString[]::new);

                return gf.createMultiLineString(arr);
            }

            // ---------- MultiPolygon ----------
            case "MultiPolygon": {

                List<?> polys = asList(coordsObj);

                Polygon[] arr = polys.stream()
                        .map(polyObj -> {

                            List<?> poly = asList(polyObj);

                            LinearRing shell =
                                    gf.createLinearRing(
                                            toCoords(asList(poly.get(0)))
                                    );

                            LinearRing[] holes =
                                    new LinearRing[Math.max(0, poly.size() - 1)];

                            for (int i = 1; i < poly.size(); i++) {
                                holes[i - 1] =
                                        gf.createLinearRing(
                                                toCoords(asList(poly.get(i)))
                                        );
                            }

                            return gf.createPolygon(shell, holes);
                        })
                        .toArray(Polygon[]::new);

                return gf.createMultiPolygon(arr);
            }

            default:
                throw new IllegalArgumentException(
                        "Unsupported geometry type: " + type
                );
        }
    }
    private Coordinate[] toCoords(List<?> points) {

        Coordinate[] coords = new Coordinate[points.size()];

        for (int i = 0; i < points.size(); i++) {

            Object pointObj = points.get(i);

            if (pointObj instanceof String) {

                String s = ((String) pointObj)
                        .replace("[", "")
                        .replace("]", "");

                String[] xy = s.split(",");

                coords[i] = new Coordinate(
                        Double.parseDouble(xy[0].trim()),
                        Double.parseDouble(xy[1].trim())
                );

            } else {

                List<?> pt = asList(pointObj);

                coords[i] = new Coordinate(
                        toDouble(pt.get(0)),
                        toDouble(pt.get(1))
                );
            }
        }

        return coords;
    }

    private double toDouble(Object obj) {

        if (obj instanceof Number) {
            return ((Number) obj).doubleValue();
        }

        if (obj instanceof String) {
            return Double.parseDouble((String) obj);
        }

        throw new IllegalArgumentException(
                "Cannot convert to double: " + obj.getClass()
        );
    }
}
