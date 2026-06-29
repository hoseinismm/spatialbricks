package ir.smh.spatialbricks.udf;

import org.locationtech.jts.algorithm.Orientation;
import org.locationtech.jts.geom.*;
import java.util.*;


public class ParseGeometryForSpatial {

    private static Map<String, Double> coordToMap(Coordinate c) {
        Map<String, Double> map = new HashMap<>();
        map.put("x", c.getX());
        map.put("y", c.getY());
        return map;
    }

    private static List<Map<String, Double>> coordinatesToMapList(List<Coordinate> coordinates) {
        List<Map<String, Double>> result = new ArrayList<>();
        for (Coordinate c : coordinates) {
            result.add(coordToMap(c));
        }
        return result;
    }

    private static List<Map<String, Double>> lineStringToCoordinates(LineString line) {
        return coordinatesToMapList(Arrays.asList(line.getCoordinates()));
    }


    public static Map<String, Object> parseGeometry(Geometry geometry)  {

        Map<String, Object> geomMap = new HashMap<>();
        List<List<Map<String, Double>>> parts = new ArrayList<>();

        if (geometry == null) {
            return null;
        }

        geomMap.put("type", geometryTypeToInt(geometry));

        if (geometry instanceof Point p) {
            parts.add(Collections.singletonList(coordToMap(p.getCoordinate())));

        } else if (geometry instanceof MultiPoint mp) {
            for (int i = 0; i < mp.getNumGeometries(); i++) {
                Point p = (Point) mp.getGeometryN(i);
                parts.add(Collections.singletonList(coordToMap(p.getCoordinate())));
            }

        } else if (geometry instanceof LineString) {
            parts.add(lineStringToCoordinates((LineString) geometry));

        } else if (geometry instanceof MultiLineString mline) {
            for (int i = 0; i < mline.getNumGeometries(); i++) {
                LineString line = (LineString) mline.getGeometryN(i);
                parts.add(lineStringToCoordinates(line));
            }

        } else if (geometry instanceof Polygon poly) {
            // حلقه بیرونی  باید CCW باشد
            List<Coordinate> shellCoords = Arrays.asList(poly.getExteriorRing().getCoordinates());
            shellCoords = ensureCCW(shellCoords);
            parts.add(coordinatesToMapList(shellCoords));

            // حلقه‌های داخلی باید CW باشند
            for (int i = 0; i < poly.getNumInteriorRing(); i++) {
                List<Coordinate> holeCoords = Arrays.asList(poly.getInteriorRingN(i).getCoordinates());
                holeCoords = ensureCW(holeCoords);
                parts.add(coordinatesToMapList(holeCoords));
            }

        } else if (geometry instanceof MultiPolygon mp) {
            for (int i = 0; i < mp.getNumGeometries(); i++) {
                Polygon poly = (Polygon) mp.getGeometryN(i);
                // حلقه بیرونی
                List<Coordinate> shellCoords = Arrays.asList(poly.getExteriorRing().getCoordinates());
                shellCoords = ensureCCW(shellCoords);
                parts.add(coordinatesToMapList(shellCoords));

                // حلقه‌های داخلی
                for (int j = 0; j < poly.getNumInteriorRing(); j++) {
                    List<Coordinate> holeCoords = Arrays.asList(poly.getInteriorRingN(j).getCoordinates());
                    holeCoords = ensureCW(holeCoords);
                    parts.add(coordinatesToMapList(holeCoords));
                }
            }
        }

        geomMap.put("parts", parts);
        return geomMap;
    }

    private static int geometryTypeToInt(Geometry geometry) {
        if (geometry instanceof Point) return 1;
        if (geometry instanceof LineString) return 2;
        if (geometry instanceof Polygon) return 3;
        if (geometry instanceof MultiPoint) return 4;
        if (geometry instanceof MultiLineString) return 5;
        if (geometry instanceof MultiPolygon) return 6;
        return 0;
    }

    // تبدیل لیست به CCW ( برای سطح توپر)
    static List<Coordinate> ensureCCW(List<Coordinate> coords) {
        if (Orientation.isCCW(coords.toArray(new Coordinate[0]))) {
            return coords;
        }
        List<Coordinate> reversed = new ArrayList<>(coords);
        Collections.reverse(reversed);
        return reversed;
    }

    // تبدیل لیست به CW (برای حفره)
    static List<Coordinate> ensureCW(List<Coordinate> coords) {
        if (!Orientation.isCCW(coords.toArray(new Coordinate[0]))) {
            return coords;
        }
        List<Coordinate> reversed = new ArrayList<>(coords);
        Collections.reverse(reversed);
        return reversed;
    }



}


