package ir.smh.spatialbricks.decoder;

import org.apache.spark.sql.Row;
import org.locationtech.jts.geom.*;

import java.util.ArrayList;
import java.util.List;

public class FlattenSpatialParquetDecoder {

    private static final GeometryFactory GF = new GeometryFactory();

    public static Geometry geometryToJTS(Row row) {

        if (row == null) {
            return null;
        }

        int type = row.getInt(0);

        double[] x = toDoubleArray(row.getList(1));
        double[] y = toDoubleArray(row.getList(2));
        int[] parts = toIntArray(row.getList(3));

        CoordinateSequenceFactory csf =
                GF.getCoordinateSequenceFactory();

        return switch (type) {

            case 1 -> { // Point

                CoordinateSequence seq = csf.create(1, 2);
                seq.setOrdinate(0, 0, x[0]);
                seq.setOrdinate(0, 1, y[0]);

                yield GF.createPoint(seq);
            }

            case 2 -> { // LineString

                CoordinateSequence seq =
                        createSequence(csf, x, y, 0, x.length);

                yield GF.createLineString(seq);
            }

            case 3 -> decodePolygon(parts, x, y, csf);

            case 4 -> decodeMultiPoint(x, y);

            case 5 -> decodeMultiLineString(parts, x, y, csf);

            case 6 -> decodeMultiPolygon(parts, x, y, csf);

            default -> null;
        };
    }

    private static MultiPoint decodeMultiPoint(
            double[] x,
            double[] y) {

        Point[] points = new Point[x.length];

        for (int i = 0; i < x.length; i++) {

            CoordinateSequence seq = GF.getCoordinateSequenceFactory().create(1, 2);
            seq.setOrdinate(0, 0, x[i]);
            seq.setOrdinate(0, 1, y[i]);

            points[i] = GF.createPoint(seq);
        }

        return GF.createMultiPoint(points);
    }

    private static MultiLineString decodeMultiLineString(
            int[] parts,
            double[] x,
            double[] y,
            CoordinateSequenceFactory csf) {

        LineString[] lines = new LineString[parts.length];

        for (int i = 0; i < parts.length; i++) {

            int start = Math.abs(parts[i]);

            int end = (i + 1 < parts.length)
                    ? Math.abs(parts[i + 1])
                    : x.length;

            CoordinateSequence seq =
                    createSequence(csf, x, y, start, end);

            lines[i] = GF.createLineString(seq);
        }

        return GF.createMultiLineString(lines);
    }

    private static Polygon decodePolygon(
            int[] parts,
            double[] x,
            double[] y,
            CoordinateSequenceFactory csf) {

        LinearRing shell = null;
        LinearRing[] holesTmp = new LinearRing[Math.max(0, parts.length - 1)];
        int h = 0;

        for (int i = 0; i < parts.length; i++) {

            int start = Math.abs(parts[i]);

            int end = (i + 1 < parts.length)
                    ? Math.abs(parts[i + 1])
                    : x.length;

            CoordinateSequence seq =
                    createSequence(csf, x, y, start, end);

            LinearRing ring = GF.createLinearRing(seq);

            if (parts[i] >= 0) {
                shell = ring;
            } else {
                holesTmp[h++] = ring;
            }
        }

        LinearRing[] holes;

        if (h == 0) {
            return GF.createPolygon(shell);
        } else {
            holes = new LinearRing[h];
            System.arraycopy(holesTmp, 0, holes, 0, h);
            return GF.createPolygon(shell, holes);
        }
    }

    private static MultiPolygon decodeMultiPolygon(
            int[] parts,
            double[] x,
            double[] y,
            CoordinateSequenceFactory csf) {

        List<Polygon> polygons = new ArrayList<>();

        LinearRing currentShell = null;
        List<LinearRing> currentHoles = new ArrayList<>();

        for (int i = 0; i < parts.length; i++) {

            int start = Math.abs(parts[i]);

            int end = (i + 1 < parts.length)
                    ? Math.abs(parts[i + 1])
                    : x.length;

            CoordinateSequence seq =
                    createSequence(csf, x, y, start, end);

            LinearRing ring = GF.createLinearRing(seq);

            if (parts[i] >= 0) {

                if (currentShell != null) {
                    polygons.add(
                            GF.createPolygon(
                                    currentShell,
                                    currentHoles.toArray(
                                            new LinearRing[0]))
                    );
                }

                currentShell = ring;
                currentHoles.clear();

            } else {

                currentHoles.add(ring);
            }
        }

        if (currentShell != null) {
            polygons.add(
                    GF.createPolygon(
                            currentShell,
                            currentHoles.toArray(
                                    new LinearRing[0]))
            );
        }

        return GF.createMultiPolygon(
                polygons.toArray(new Polygon[0]));
    }

    private static CoordinateSequence createSequence(
            CoordinateSequenceFactory csf,
            double[] x,
            double[] y,
            int start,
            int end) {

        int size = end - start;

        CoordinateSequence seq = csf.create(size, 2);

        for (int i = 0; i < size; i++) {

            seq.setOrdinate(i, 0, x[start + i]);
            seq.setOrdinate(i, 1, y[start + i]);
        }

        return seq;
    }

    private static double[] toDoubleArray(List<Double> list) {

        if (list == null || list.isEmpty()) {
            return new double[0];
        }

        double[] arr = new double[list.size()];

        for (int i = 0; i < arr.length; i++) {
            arr[i] = list.get(i);
        }

        return arr;
    }

    private static int[] toIntArray(List<Integer> list) {

        if (list == null || list.isEmpty()) {
            return new int[0];
        }

        int[] arr = new int[list.size()];

        for (int i = 0; i < arr.length; i++) {
            arr[i] = list.get(i);
        }

        return arr;
    }
}
