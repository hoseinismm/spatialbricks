package ir.smh.spatialbricks.decoder;

import org.apache.spark.sql.Row;
import org.locationtech.jts.algorithm.Orientation;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.impl.PackedCoordinateSequenceFactory;
import java.util.ArrayList;
import java.util.List;

public class GeometryDecoder2 {

    private static final PackedCoordinateSequenceFactory sequenceFactory =
            PackedCoordinateSequenceFactory.DOUBLE_FACTORY;

    private static final GeometryFactory geometryFactory = new GeometryFactory();

    // استفاده از ایندکس‌های عددی ثابت برای دور زدن عملیات سنگین جستجوی اسکیما (fieldIndex)
    private static final int TYPE_IDX = 0;
    private static final int PARTS_IDX = 1;
    private static final int COORDS_IDX = 0; // در ساختار partType اولین فیلد coordinates است
    private static final int X_IDX = 0;      // در coordType اولین فیلد x است
    private static final int Y_IDX = 1;      // در coordType دومین فیلد y است

    public static Geometry geometryToJTS(Row geoRow) {
        if (geoRow == null) return null;

        int type = geoRow.getInt(TYPE_IDX);
        List<Row> parts = geoRow.getList(PARTS_IDX);

        if (parts == null || parts.isEmpty()) {
            throw new IllegalArgumentException("Geometry contains no parts");
        }

        switch (type) {
            case 1:
                CoordinateSequence pointSeq = extractCoordsSequence(parts.get(0));
//                if (pointSeq.size() != 1) {
//                    throw new IllegalArgumentException("Point must contain exactly one coordinate");
//                }
                return geometryFactory.createPoint(pointSeq);

            case 2:
                return geometryFactory.createLineString(extractCoordsSequence(parts.get(0)));

            case 3:
                return decodePolygon(parts);

            case 4:
                return decodeMultiPoint(parts);

            case 5:
                return decodeMultiLineString(parts);

            case 6:
                return decodeMultiPolygon(parts);

            default:
                throw new IllegalArgumentException("Unsupported geometry type: " + type);
        }
    }

    private static MultiPoint decodeMultiPoint(List<Row> parts) {
        int size = parts.size();
        Point[] points = new Point[size];
        for (int i = 0; i < size; i++) {
            CoordinateSequence seq = extractCoordsSequence(parts.get(i));
//            if (seq.size() != 1) {
//                throw new IllegalArgumentException("MultiPoint part must contain exactly one coordinate");
//            }
            points[i] = geometryFactory.createPoint(seq);
        }
        return geometryFactory.createMultiPoint(points);
    }

    private static MultiLineString decodeMultiLineString(List<Row> parts) {
        int size = parts.size();
        LineString[] lines = new LineString[size];
        for (int i = 0; i < size; i++) {
            lines[i] = geometryFactory.createLineString(extractCoordsSequence(parts.get(i)));
        }
        return geometryFactory.createMultiLineString(lines);
    }

    private static Polygon decodePolygon(List<Row> parts) {
        int size = parts.size();
        LinearRing shell = geometryFactory.createLinearRing(extractCoordsSequence(parts.get(0)));

        LinearRing[] holes = new LinearRing[size - 1];
        for (int i = 1; i < size; i++) {
            holes[i - 1] = geometryFactory.createLinearRing(extractCoordsSequence(parts.get(i)));
        }

        return geometryFactory.createPolygon(shell, holes);
    }

    private static MultiPolygon decodeMultiPolygon(List<Row> parts) {
        List<Polygon> polygons = new ArrayList<>();
        LinearRing currentShell = null;
        List<LinearRing> currentHoles = new ArrayList<>();

        for (int i = 0; i < parts.size(); i++) {
            Row ringPart = parts.get(i);
            CoordinateSequence ringSeq = extractCoordsSequence(ringPart);
            LinearRing ring = geometryFactory.createLinearRing(ringSeq);

            if (isShell(ringSeq)) {
                if (currentShell != null) {
                    polygons.add(geometryFactory.createPolygon(currentShell, currentHoles.toArray(new LinearRing[0])));
                    currentHoles.clear();
                }
                currentShell = ring;
            } else {
                if (currentShell == null) {
                    throw new IllegalArgumentException("Encountered hole before shell in MultiPolygon");
                }
                currentHoles.add(ring);
            }
        }

        if (currentShell != null) {
            polygons.add(geometryFactory.createPolygon(currentShell, currentHoles.toArray(new LinearRing[0])));
        }

        return geometryFactory.createMultiPolygon(polygons.toArray(new Polygon[0]));
    }

    private static CoordinateSequence extractCoordsSequence(Row part) {

        scala.collection.Seq<?> rawCoords = part.getSeq(COORDS_IDX);
        int size = rawCoords.size();

        double[] flatCoords = new double[size * 2];

        int idx = 0;
        for (int i = 0; i < size; i++) {
            Row c = (Row) rawCoords.apply(i);
            flatCoords[idx++] = c.getDouble(X_IDX);
            flatCoords[idx++] = c.getDouble(Y_IDX);
        }

        return sequenceFactory.create(flatCoords, 2);
    }

    private static boolean isShell(CoordinateSequence seq) {

        return Orientation.isCCW(seq);
    }
}