package ir.smh.spatialbricks.encoder.udf;

import ir.smh.spatialbricks.encoder.GeometryResult;
import org.locationtech.jts.geom.*;

import java.util.*;

public class ParseGeometry {


    private static Map<String, Object> coordToMap(Coordinate c) {
        Map<String, Object> map = new HashMap<>();
        map.put("x", c.getX());
        map.put("y", c.getY());
        return map;
    }

    private static List<Map<String, Object>> lineStringToCoordinates(LineString line) {
        List<Map<String, Object>> coords = new ArrayList<>();
        for (Coordinate c : line.getCoordinates()) {
            coords.add(coordToMap(c));
        }
        return coords;
    }


    public static GeometryResult parseGeometry(Geometry geometry) throws Exception {
        GeometryResult result = new GeometryResult();

        result.geometry = geometry;

        result.geomMap = new HashMap<>();
        result.geomMap.put("type", geometryTypeToInt(result.geometry)); // عددی طبق SpatialParquet

        List<List<Map<String, Object>>> parts = new ArrayList<>();

        if (result.geometry instanceof Point) {
            Point p = (Point) result.geometry;
            parts.add(Collections.singletonList(coordToMap(p.getCoordinate())));

        } else if (result.geometry instanceof MultiPoint) {
            MultiPoint mp = (MultiPoint) result.geometry;
            for (int i = 0; i < mp.getNumGeometries(); i++) {
                Point p = (Point) mp.getGeometryN(i);
                parts.add(Collections.singletonList(coordToMap(p.getCoordinate())));
            }

        } else if (result.geometry instanceof LineString) {
            parts.add(lineStringToCoordinates((LineString) result.geometry));

        } else if (result.geometry instanceof MultiLineString) {
            MultiLineString mline = (MultiLineString) result.geometry;
            for (int i = 0; i < mline.getNumGeometries(); i++) {
                LineString line = (LineString) mline.getGeometryN(i);
                parts.add(lineStringToCoordinates(line));
            }

        } else if (result.geometry instanceof Polygon) {
            Polygon poly = (Polygon) result.geometry;
            // حلقه بیرونی
            parts.add(lineStringToCoordinates(poly.getExteriorRing()));
            // حلقه‌های داخلی (سوراخ‌ها)
            for (int i = 0; i < poly.getNumInteriorRing(); i++) {
                parts.add(lineStringToCoordinates(poly.getInteriorRingN(i)));
            }

        } else if (result.geometry instanceof MultiPolygon) {
            MultiPolygon mp = (MultiPolygon) result.geometry;
            for (int i = 0; i < mp.getNumGeometries(); i++) {
                Polygon poly = (Polygon) mp.getGeometryN(i);
                // حلقه بیرونی
                parts.add(lineStringToCoordinates(poly.getExteriorRing()));
                // حلقه‌های داخلی
                for (int j = 0; j < poly.getNumInteriorRing(); j++) {
                    parts.add(lineStringToCoordinates(poly.getInteriorRingN(j)));
                }
            }
        }

        result.geomMap.put("part", parts);
        return result;
    }

    private static int geometryTypeToInt(Geometry geometry) {
        if (geometry instanceof Point) return 1;
        if (geometry instanceof LineString) return 2;
        if (geometry instanceof Polygon) return 3;
        if (geometry instanceof MultiPoint) return 4;
        if (geometry instanceof MultiLineString) return 5;
        if (geometry instanceof MultiPolygon) return 6;
        return 0; // empty or unknown
    }
}


