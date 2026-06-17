package ir.smh.spatialbricks.encoder.udf;

import org.locationtech.jts.geom.*;

import java.util.*;

public class ParseGeometry2 {


    public static Map<String, Object> parseGeometry(Geometry geometry) {

        if (geometry == null) {
            return null;
        }

        int idx = 0;
        List<Integer> partlist;

        int n = geometry.getNumPoints();
        double[] xcoords = new double[n];
        double[] ycoords = new double[n];

        Map<String, Object> geomMap = new HashMap<>();

        geomMap.put("type", geometryTypeToInt(geometry));

        if (n == 0) {
            return geomMap;
        }

        if (geometry instanceof Point p) {
            partlist = new ArrayList<>();
            Coordinate c = p.getCoordinate();
            xcoords[0] = c.x;
            ycoords[0] = c.y;

        } else if (geometry instanceof MultiPoint mp) {
            partlist = new ArrayList<>();
            Coordinate[] coords = mp.getCoordinates();
            for (int i = 0; i < n; i++) {
                xcoords[i] = coords[i].x;
                ycoords[i] = coords[i].y;
            }

        } else if (geometry instanceof LineString ls) {
            partlist = new ArrayList<>();
            Coordinate[] coords = ls.getCoordinates();

            for (int i = 0; i < coords.length; i++) {
                xcoords[i] = coords[i].x;
                ycoords[i] = coords[i].y;
            }

        } else if (geometry instanceof MultiLineString mline) {
            int g=geometry.getNumGeometries();
            partlist = new ArrayList<>(g);
            for (int i = 0; i < mline.getNumGeometries(); i++) {
                LineString line = (LineString) mline.getGeometryN(i);
                Coordinate[] coords = line.getCoordinates();
                for (int j = 0; j < coords.length; j++) {
                    xcoords[idx + j] = coords[j].x;
                    ycoords[idx + j] = coords[j].y;
                }
                partlist.add(idx);
                idx = idx + coords.length;
            }

        } else if (geometry instanceof Polygon poly) {
            partlist = new ArrayList<>(100);
            Coordinate[] ext = poly.getExteriorRing().getCoordinates();
            partlist.add(idx);
            idx += copyCoords(ext, xcoords, ycoords, idx);

            for (int i = 0; i < poly.getNumInteriorRing(); i++) {
                Coordinate[] coords = poly.getInteriorRingN(i).getCoordinates();
                partlist.add(-idx);
                idx += copyCoords(coords, xcoords, ycoords, idx);
            }

        } else if (geometry instanceof MultiPolygon mp) {
            partlist = new ArrayList<>(1000);
            for (int i = 0; i < mp.getNumGeometries(); i++) {
                Polygon poly = (Polygon) mp.getGeometryN(i);
                Coordinate[] ext = poly.getExteriorRing().getCoordinates();
                partlist.add(idx);
                idx += copyCoords(ext, xcoords, ycoords, idx);

                for (int j = 0; j < poly.getNumInteriorRing(); j++) {
                    Coordinate[] coords = poly.getInteriorRingN(j).getCoordinates();
                    partlist.add(-idx);
                    idx += copyCoords(coords, xcoords, ycoords, idx);
                }
            }
        } else return null;

        geomMap.put("x", xcoords);
        geomMap.put("y", ycoords);
        Integer[] parts = partlist.toArray(new Integer[0]);
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
    private static int copyCoords(Coordinate[] coords, double[] x, double[] y, int idx) {
        for (int i = 0; i < coords.length; i++) {
            x[idx + i] = coords[i].x;
            y[idx + i] = coords[i].y;
        }
        return coords.length;
    }
}



