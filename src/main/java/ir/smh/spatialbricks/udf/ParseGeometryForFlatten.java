package ir.smh.spatialbricks.udf;

import org.locationtech.jts.geom.*;

import java.util.*;

public class ParseGeometryForFlatten {

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

            partlist = new ArrayList<>(1);

            Coordinate c = p.getCoordinate();
            xcoords[0] = c.x;
            ycoords[0] = c.y;

        } else if (geometry instanceof MultiPoint mp) {

            partlist = new ArrayList<>(mp.getNumGeometries());

            for (int i = 0; i < mp.getNumGeometries(); i++) {

                Point p = (Point) mp.getGeometryN(i);
                CoordinateSequence seq = p.getCoordinateSequence();

                xcoords[idx] = seq.getX(0);
                ycoords[idx] = seq.getY(0);

                partlist.add(idx);
                idx++;
            }
        } else if (geometry instanceof LineString ls) {

            partlist = new ArrayList<>(1);

            CoordinateSequence seq = ls.getCoordinateSequence();

            int size = seq.size();
            for (int i = 0; i < size; i++) {
                xcoords[idx + i] = seq.getX(i);
                ycoords[idx + i] = seq.getY(i);
            }

            partlist.add(idx);
            idx += size;

        } else if (geometry instanceof MultiLineString mline) {

            partlist = new ArrayList<>(mline.getNumGeometries());

            for (int i = 0; i < mline.getNumGeometries(); i++) {

                LineString line = (LineString) mline.getGeometryN(i);
                CoordinateSequence seq = line.getCoordinateSequence();

                partlist.add(idx);

                int size = seq.size();
                for (int j = 0; j < size; j++) {
                    xcoords[idx + j] = seq.getX(j);
                    ycoords[idx + j] = seq.getY(j);
                }
                idx += size;
            }

        } else if (geometry instanceof Polygon poly) {

            partlist = new ArrayList<>(poly.getNumInteriorRing() + 1);

            // exterior ring
            CoordinateSequence ext = poly.getExteriorRing().getCoordinateSequence();

            partlist.add(idx);
            int extSize = ext.size();
            for (int i = 0; i < extSize; i++) {
                xcoords[idx + i] = ext.getX(i);
                ycoords[idx + i] = ext.getY(i);
            }

            idx += extSize;
            // holes
            for (int i = 0; i < poly.getNumInteriorRing(); i++) {

                LinearRing hole = poly.getInteriorRingN(i);
                CoordinateSequence seq = hole.getCoordinateSequence();

                partlist.add(-idx);
                int size = seq.size();
                for (int j = 0; j < size; j++) {
                    xcoords[idx + j] = seq.getX(j);
                    ycoords[idx + j] = seq.getY(j);
                }
                idx += size;
            }

        } else if (geometry instanceof MultiPolygon mp) {

            partlist = new ArrayList<>(mp.getNumGeometries() * 2);

            for (int i = 0; i < mp.getNumGeometries(); i++) {

                Polygon poly = (Polygon) mp.getGeometryN(i);

                // exterior
                CoordinateSequence ext = poly.getExteriorRing().getCoordinateSequence();
                partlist.add(idx);
                int extSize = ext.size();
                for (int k = 0; k < extSize; k++) {
                    xcoords[idx + k] = ext.getX(k);
                    ycoords[idx + k] = ext.getY(k);
                }

                idx += extSize;

                // holes
                for (int j = 0; j < poly.getNumInteriorRing(); j++) {
                    LinearRing hole = poly.getInteriorRingN(j);
                    CoordinateSequence seq = hole.getCoordinateSequence();

                    partlist.add(-idx);

                    int size = seq.size();
                    for (int k = 0; k < size; k++) {
                        xcoords[idx + k] = seq.getX(k);
                        ycoords[idx + k] = seq.getY(k);
                    }
                    idx += size;
                }
            }

        } else {
            return null;
        }

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
}