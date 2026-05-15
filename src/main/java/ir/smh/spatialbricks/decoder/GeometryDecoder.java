package ir.smh.spatialbricks.decoder;

import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.locationtech.jts.geom.*;
import java.util.List;
import java.util.Map;

public class GeometryDecoder {

    private static final GeometryFactory geometryFactory = new GeometryFactory();

    public static void registerDecoderUDF(SparkSession spark) {
        UDF1<Row, Geometry> structToGeometry = (Row row) -> {
            if (row == null) return null;

            try {
                int type = row.getInt(row.fieldIndex("type"));
                List<Row> parts = row.getList(row.fieldIndex("part"));

                switch (type) {
                    case 1: // POINT
                        // دسترسی به part اول
                        Row part0 = parts.get(0);

                        // گرفتن لیست مختصات از درون part
                        List<Row> coordinates = part0.getList(part0.fieldIndex("coordinate"));

                        // اولین مختصات (x, y)
                        Row coord = coordinates.get(0);

                        double x = coord.getDouble(coord.fieldIndex("x"));
                        double y = coord.getDouble(coord.fieldIndex("y"));

                        return geometryFactory.createPoint(new Coordinate(x, y));

                    case 2: // LINESTRING
                        List<Row> coords = parts.get(0).getList(0); // فقط یک part برای خط
                        Coordinate[] lineCoords = coords.stream()
                                .map(r -> new Coordinate(r.getDouble(r.fieldIndex("x")), r.getDouble(r.fieldIndex("y"))))
                                .toArray(Coordinate[]::new);
                        return geometryFactory.createLineString(lineCoords);

                    case 3: // POLYGON
                        // ممکن است چند part برای حلقه‌ها (exterior + interiors) داشته باشد
                        LinearRing shell = null;
                        LinearRing[] holes = new LinearRing[parts.size() - 1];
                        for (int i = 0; i < parts.size(); i++) {
                            List<Row> ringCoords = parts.get(i).getList(0);
                            Coordinate[] coordsArr = ringCoords.stream()
                                    .map(r -> new Coordinate(r.getDouble(r.fieldIndex("x")), r.getDouble(r.fieldIndex("y"))))
                                    .toArray(Coordinate[]::new);
                            LinearRing ring = geometryFactory.createLinearRing(coordsArr);
                            if (i == 0) shell = ring;
                            else holes[i - 1] = ring;
                        }
                        return geometryFactory.createPolygon(shell, holes);

                    default:
                        throw new IllegalArgumentException("Unsupported geometry type: " + type);
                }
            } catch (Exception e) {
                e.printStackTrace();
                return null;
            }
        };

        spark.udf().register("decodeGeometry", structToGeometry, org.apache.spark.sql.types.DataTypes.BinaryType);
    }
}
