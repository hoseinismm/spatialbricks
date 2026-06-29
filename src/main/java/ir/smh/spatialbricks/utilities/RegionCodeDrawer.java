package ir.smh.spatialbricks.utilities;


import ir.smh.spatialbricks.core.BucketManagerForBboxIndexing;
import org.locationtech.jts.geom.*;

import java.io.IOException;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.*;


import static ir.smh.spatialbricks.core.BucketManagerForBboxIndexing.decode;
import static ir.smh.spatialbricks.core.BucketManagerForBboxIndexing.initialBucket;
import ir.smh.spatialbricks.core.BucketManagerForBboxIndexing.Bucket;
import org.wololo.geojson.GeoJSON;
import org.wololo.jts2geojson.GeoJSONWriter;

public class RegionCodeDrawer {

    private static final Pattern PATTERN =
            Pattern.compile("region_code=(\\d+)");

    public static MultiPolygon readRegions(
            String directory,
            Bucket root,
            GeometryFactory gf) throws IOException {

        List<Polygon> polygons = new ArrayList<>();

        try (DirectoryStream<Path> stream =
                     Files.newDirectoryStream(Paths.get(directory))) {

            for (Path path : stream) {

                if (!Files.isDirectory(path))
                    continue;

                Matcher matcher =
                        PATTERN.matcher(path.getFileName().toString());

                if (!matcher.find())
                    continue;

                long code = Long.parseLong(matcher.group(1));

                Bucket bucket = decode(code, root);

                Polygon polygon = createPolygon(bucket, gf);

                polygons.add(polygon);
            }
        }

        return gf.createMultiPolygon(
                polygons.toArray(new Polygon[0]));
    }

    private static Polygon createPolygon(
            Bucket bucket,
            GeometryFactory gf) {

        Coordinate[] coords = new Coordinate[]{
                new Coordinate(bucket.xmin, bucket.ymin),
                new Coordinate(bucket.xmax, bucket.ymin),
                new Coordinate(bucket.xmax, bucket.ymax),
                new Coordinate(bucket.xmin, bucket.ymax),
                new Coordinate(bucket.xmin, bucket.ymin)
        };

        return gf.createPolygon(coords);
    }
    public static void main(String[] args) throws IOException {
        GeometryFactory gf = new GeometryFactory();

        Bucket root = BucketManagerForBboxIndexing.loadBucket(
                "D:\\payannameh\\datasets\\nyc_taxi\\bucket_flattenSilverIndexed_nyc_taxi.gz"
        );

        if (root == null) {
            throw new RuntimeException("Bucket could not be loaded.");
        }

        MultiPolygon mp = readRegions(
                "D:\\payannameh\\datasets\\nyc_taxi\\flattenSilverIndexed\\nyc_taxi\\data",
                root,
                gf);

        System.out.println("Buckets: " + mp.getNumGeometries());
        GeoJSONWriter writer = new GeoJSONWriter();

        GeoJSON json = writer.write(mp);

        Files.writeString(
                Paths.get("regions.geojson"),
                json.toString()
        );
    }
}
