package ir.smh.spatialbricks.decoder;

import org.apache.spark.sql.Row;
import org.locationtech.jts.geom.*;
import java.util.ArrayList;
import java.util.List;

public class GeometryDecoder {

    private static final GeometryFactory geometryFactory = new GeometryFactory();

    public static Geometry rowToGeometry(Row row) {

        if (row == null) return null;

        int type = row.getInt(row.fieldIndex("type"));

        List<?> rawParts = row.getList(row.fieldIndex("part"));

        List<Row> parts = new ArrayList<>();
        for (Object o : rawParts) {
            parts.add((Row) o);
        }

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

            case 3: // POLYGON

                LinearRing shell = null;
                LinearRing[] holes = new LinearRing[Math.max(0, parts.size() - 1)];

                for (int i = 0; i < parts.size(); i++) {

                    Row ringPart = parts.get(i);
                    List<?> rawRingCoords = ringPart.getList(ringPart.fieldIndex("coordinate"));

                    Coordinate[] ringCoords = new Coordinate[rawRingCoords.size()];

                    for (int j = 0; j < rawRingCoords.size(); j++) {
                        Row c = (Row) rawRingCoords.get(j);
                        ringCoords[j] = new Coordinate(
                                c.getDouble(c.fieldIndex("x")),
                                c.getDouble(c.fieldIndex("y"))
                        );
                    }

                    LinearRing ring = geometryFactory.createLinearRing(ringCoords);

                    if (i == 0)
                        shell = ring;
                    else
                        holes[i - 1] = ring;
                }

                return geometryFactory.createPolygon(shell, holes);

            default:
                throw new IllegalArgumentException("Unsupported geometry type: " + type);
        }
    }
}
