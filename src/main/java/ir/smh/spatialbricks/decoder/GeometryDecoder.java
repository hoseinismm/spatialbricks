package ir.smh.spatialbricks.decoder;

import org.apache.spark.sql.Row;
import org.locationtech.jts.algorithm.Orientation;
import org.locationtech.jts.geom.*;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class GeometryDecoder {

    private static final GeometryFactory geometryFactory = new GeometryFactory();

    public static Geometry rowToGeometry(Row rootRow) {
        if (rootRow == null) return null;


        Row geoRow = rootRow.getStruct(rootRow.fieldIndex("geometry"));
        if (geoRow == null) return null;

        // حالا بقیه کد را روی geoRow اجرا می‌کنیم
        int type = geoRow.getInt(geoRow.fieldIndex("type"));
        List<Row> parts = geoRow.getList(geoRow.fieldIndex("part"))
                .stream()
                .map(o -> (Row) o)
                .toList();

        switch (type) {

            case 1: // POINT
                Row part = parts.get(0);

                List<?> rawCoords = part.getList(part.fieldIndex("coordinate"));
                Row coord = (Row) rawCoords.get(0);

                double x = coord.getDouble(coord.fieldIndex("x"));
                double y = coord.getDouble(coord.fieldIndex("y"));

                return geometryFactory.createPoint(new Coordinate(x, y));

            case 2: // LINESTRING
                Row linePart = parts.get(0);
                List<?> rawLineCoords = linePart.getList(linePart.fieldIndex("coordinate"));

                Coordinate[] lineCoords = new Coordinate[rawLineCoords.size()];

                for (int i = 0; i < rawLineCoords.size(); i++) {
                    Row c = (Row) rawLineCoords.get(i);
                    lineCoords[i] = new Coordinate(
                            c.getDouble(c.fieldIndex("x")),
                            c.getDouble(c.fieldIndex("y"))
                    );
                }

                return geometryFactory.createLineString(lineCoords);

            case 3:
                return decodePolygon(parts);
            case 6:
                return decodeMultiPolygon(parts);
            default:
                throw new IllegalArgumentException("Unsupported geometry type: " + type);
        }
    }
    private static Polygon decodePolygon(List<Row> parts) {
        LinearRing shell = null;
        List<LinearRing> holes = new ArrayList<>();

        for (int i = 0; i < parts.size(); i++) {
            Row ringPart = parts.get(i);
            Coordinate[] ringCoords = extractCoords(ringPart);

            LinearRing ring = geometryFactory.createLinearRing(ringCoords);

            if (i == 0) {
                shell = ring;
            } else {
                holes.add(ring);
            }
        }

        if (shell == null) {
            throw new IllegalArgumentException("Polygon has no shell");
        }

        return geometryFactory.createPolygon(shell, holes.toArray(new LinearRing[0]));
    }

    private static MultiPolygon decodeMultiPolygon(List<Row> parts) {
        List<Polygon> polygons = new ArrayList<>();

        LinearRing currentShell = null;
        List<LinearRing> currentHoles = new ArrayList<>();

        for (Row ringPart : parts) {
            Coordinate[] ringCoords = extractCoords(ringPart);
            LinearRing ring = geometryFactory.createLinearRing(ringCoords);

            if (isShell(ringCoords)) {
                // اگر قبلاً یک polygon در حال ساخت داشتیم، آن را نهایی کن
                if (currentShell != null) {
                    polygons.add(
                            geometryFactory.createPolygon(
                                    currentShell,
                                    currentHoles.toArray(new LinearRing[0])
                            )
                    );
                    currentHoles.clear();
                }

                // شروع polygon جدید
                currentShell = ring;
            } else {
                // hole باید بعد از یک shell بیاید
                if (currentShell == null) {
                    throw new IllegalArgumentException("Encountered hole before shell in MultiPolygon");
                }
                currentHoles.add(ring);
            }
        }

        // آخرین polygon را هم اضافه کن
        if (currentShell != null) {
            polygons.add(
                    geometryFactory.createPolygon(
                            currentShell,
                            currentHoles.toArray(new LinearRing[0])
                    )
            );
        }

        return geometryFactory.createMultiPolygon(polygons.toArray(new Polygon[0]));
    }

    private static Coordinate[] extractCoords(Row ringPart) {
        List<?> rawCoords = ringPart.getList(ringPart.fieldIndex("coordinate"));
        Coordinate[] coords = new Coordinate[rawCoords.size()];

        for (int i = 0; i < rawCoords.size(); i++) {
            Row c = (Row) rawCoords.get(i);
            coords[i] = new Coordinate(
                    c.getDouble(c.fieldIndex("x")),
                    c.getDouble(c.fieldIndex("y"))
            );
        }

        return coords;
    }

    private static boolean isShell(Coordinate[] coords) {
        // اینجا باید مطابق استاندارد دیتای خودتان تنظیم شود
        // اگر shell ها CCW هستند:
        return Orientation.isCCW(coords);

        // اگر shell ها CW هستند، این را بگذارید:
        // return !Orientation.isCCW(coords);
    }
}
