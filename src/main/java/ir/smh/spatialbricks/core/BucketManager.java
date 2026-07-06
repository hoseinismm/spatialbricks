package ir.smh.spatialbricks.core;

import ir.smh.spatialbricks.udf.UDFRegistry;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.parser.ParseException;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;

import java.io.*;
import java.util.List;
import java.util.Scanner;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.Files;

public class BucketManager {

    public static void addGeosToBuckets(
            List<Row> rows,
            Bucket rootBucket,
            long maxSize,
            double fraction) {

        long sampleWeight = (long) Math.floor(1.0 / fraction);
        double foraddprecision = (1.0 / fraction) - sampleWeight;

        long effectiveMaxSize = (maxSize < 1) ? 1L : maxSize;

            for (Row row : rows) {

                if (row == null) {
                    continue;
                }


                Row bbox = row.getAs("bbox");

                if (bbox == null) {
                    continue;
                }

                Object minX = bbox.getAs("min_x");

                if (minX == null) {
                    continue;
                }

                double min_x = ((Number) bbox.getAs("min_x")).doubleValue();
                double min_y = ((Number) bbox.getAs("min_y")).doubleValue();
                double max_x = ((Number) bbox.getAs("max_x")).doubleValue();
                double max_y = ((Number) bbox.getAs("max_y")).doubleValue();

                Bucket current = rootBucket;

                if (bucketOutOfRange(min_x,min_y ,max_x , max_y)) {
                    System.out.println(
                            "*********this geo is out of range: "
                                    + min_x + ", " + min_y + ", " + max_x + ", " + max_y
                    );
                    continue;
                }

                while (current.hasChildren) {

                    if (max_x <= current.xmid && min_y >= current.ymid) {
                        current = current.topleft;
                    } else if (min_x >= current.xmid && min_y >= current.ymid) {
                        current = current.topright;
                    } else if (max_x <= current.xmid && max_y <= current.ymid) {
                        current = current.bottomleft;
                    } else if (min_x >= current.xmid && max_y <= current.ymid) {
                        current = current.bottomright;
                    } else break;
                }

                if ((current.count >= effectiveMaxSize)
                        && (current.xmax - current.xmin > 0.0001) && !current.hasChildren) {

                    if (max_x <= current.xmid && min_y >= current.ymid) {
                        current.createChild();
                        current = current.topleft;
                    } else if (min_x >= current.xmid && min_y >= current.ymid) {
                        current.createChild();
                        current = current.topright;
                    } else if (max_x <= current.xmid && max_y <= current.ymid) {
                        current.createChild();
                        current = current.bottomleft;
                    } else if (min_x >= current.xmid && max_y <= current.ymid) {
                        current.createChild();
                        current = current.bottomright;
                    }
                }

                current.count += sampleWeight;
                current.fraction += foraddprecision;
                if (current.fraction >= 1) {
                    current.count++;
                    current.fraction -= 1;
                }
            }
    }

    public static class Bucket implements Serializable {
        @Serial
        private static final long serialVersionUID = 1L;

        public double xmin;
        public double xmax;
        public double ymin;
        public double ymax;
        public double xmid;
        public double ymid;
        public long count;
        public Bucket topright;
        public Bucket topleft;
        public Bucket bottomright;
        public Bucket bottomleft;
        public boolean hasChildren;
        public double fraction;
        public long code;
        public Long snapshot;


        Bucket(double xmin, double ymin, double xmax, double ymax, long code) {

            this.xmin = xmin;
            this.xmax = xmax;
            this.xmid = xmin + (xmax - xmin) / 2;
            this.ymin = ymin;
            this.ymax = ymax;
            this.ymid = ymin + (ymax - ymin) / 2;
            this.count = 0;
            this.hasChildren = false;
            this.fraction = 0;
            this.code = code;
            this.snapshot = null;
        }

        public void createChild() {
            this.topleft = new Bucket(this.xmin,this.ymid, this.xmid,  this.ymax, this.code * 4 + 1);
            this.topright = new Bucket(this.xmid, this.ymid, this.xmax, this.ymax, this.code * 4);
            this.bottomleft = new Bucket(this.xmin, this.ymin, this.xmid, this.ymid, this.code * 4 + 3);
            this.bottomright = new Bucket(this.xmid, this.ymin, this.xmax, this.ymid, this.code * 4 + 2);
            this.hasChildren = true;
        }
    }

    public static void saveBucket(Bucket bucket, String filename) {

        try (ObjectOutputStream out =
                     new ObjectOutputStream(
                             new GZIPOutputStream(
                                     new FileOutputStream(filename)))) {

            out.writeObject(bucket);

        } catch (IOException e) {
            throw new IllegalStateException(
                    "Failed to save bucket to: " + filename, e);
        }
    }


    public static Bucket loadBucket(String filename) {

        Path path = Path.of(filename);

        if (!Files.exists(path)) {
            return null;
        }

        try (ObjectInputStream in = new ObjectInputStream(
                new GZIPInputStream(
                        Files.newInputStream(path)))) {

            return (Bucket) in.readObject();

        } catch (IOException | ClassNotFoundException e) {
            throw new IllegalStateException(
                    "Failed to load bucket from: " + filename, e);
        }
    }

    public static Bucket initialBucket(double min_x, double min_y, double max_x, double max_y) {
        Bucket initial = new Bucket(min_x, min_y,max_x, max_y, 1);
        splitbucket(initial);
        System.out.println("bucketinitial created");
        return initial;
    }

    private static void splitbucket(Bucket bucket) {
        if (bucket.xmax-bucket.xmin>24) {
            //System.out.println(bucket.min+"  "+bucket.max+" "+bucket.mid);
            bucket.createChild();
            splitbucket(bucket.topleft);
            splitbucket(bucket.topright);
            splitbucket(bucket.bottomleft);
            splitbucket(bucket.bottomright);
        }
    }

    private static boolean bucketOutOfRange(double xmin,  double ymin, double xmax, double ymax) {
        return !(  xmin >= -180 && ymin <= 90 && xmax <= 180 && ymax >= -90);
    }

    public static Bucket computeBucketBorders(
            SparkSession spark,
            Dataset<Row> df,
            TableSpec silver,
            long rowsCapableOfProcessingByDriver,
            long maxPartitionSize,
            UDFRegistry<?,?> udfRegistry) throws NoSuchTableException, ParseException {

        Path bucketPath = Paths.get(
                silver.path(),
                String.format(
                        "bucket_%s_%s.gz",
                        silver.database(),
                        silver.table()
                )
        );

        long totalRows = df.count();

        double fraction =
                Math.min(
                        1.0,
                        (double) rowsCapableOfProcessingByDriver / totalRows
                );

        udfRegistry.registerBboxUdf();

        Dataset<Row> bboxDf = df
                .sample(false, fraction)
                .select(
                        callUDF(
                                "calculateBbox",
                                col("geometry")
                        ).alias("bbox")
                );

        List<Row> rows = bboxDf.collectAsList();

        Bucket loadedBucket;
        Bucket rootBucket;
        if (Files.exists(bucketPath)) {

            loadedBucket =
                    loadBucket(
                            bucketPath.toString()
                    );
            if (loadedBucket == null) {
                throw new IllegalStateException("Loaded bucket is null.");
            }
            String tableName = silver.database() + "." + silver.table();
            boolean exists = spark.catalog().tableExists(tableName);
            if (exists) {
                Table table = Spark3Util
                        .loadIcebergTable(spark, tableName);

                Snapshot snapshot = table.currentSnapshot();

                if (snapshot == null) {
                    System.out.println("""
                                Table has no current snapshot.
                                Saved bucket is considered invalid.
                                Rebuilding bucket...
                            """);

                    rootBucket = initialBucket(
                            -180.0,
                            -90.0,
                            180.0,
                            90.0
                    );
                } else  if (loadedBucket.snapshot == snapshot.snapshotId()) {
                    rootBucket = loadedBucket;
                    System.out.println(
                            "Bucket loaded from: "
                                    + bucketPath + " Successfully"
                    );
                } else {
                    System.out.println("""
                            Current snapshot of table differs from saved bucket.
                            [C] Continue with saved bucket
                            [R] Rebuild bucket
                            [Any other key] Terminate
                            """);

                    Scanner scanner = new Scanner(System.in);
                    String choice = scanner.nextLine().trim();

                    if (choice.equalsIgnoreCase("C")) {

                        rootBucket = loadedBucket;

                        System.out.println(
                                "Continue using saved bucket: " + bucketPath
                        );

                    } else if (choice.equalsIgnoreCase("R")) {

                        rootBucket = initialBucket(
                                -180.0,
                                -90.0,
                                180.0,
                                90.0
                        );

                        System.out.println(
                                "Rebuilding bucket from scratch."
                        );

                    } else {

                        throw new IllegalStateException(
                                "Operation cancelled by user."
                        );
                    }
                }

            } else {

                rootBucket =
                        initialBucket(
                                -180.0,
                                -90.0,
                                180.0,
                                90.0
                        );

                System.out.println(
                        "Created new bucket for: "
                                + bucketPath
                );
            }
        } else  {

            rootBucket =
                    initialBucket(
                            -180.0,
                            -90.0,
                            180.0,
                            90.0
                    );

            System.out.println(
                    "Created new bucket for: "
                            + bucketPath
            );
        }

        if (fraction != 0.0) {

            addGeosToBuckets(
                    rows,
                    rootBucket,
                    maxPartitionSize,
                    fraction
            );
        }

        return rootBucket;
    }

    public static void updateTreeFromStats(List<Row> rows, Bucket root) {
        for (Row row : rows) {

            Long f = row.getAs("region_code");
            Long count = row.getAs("total_count");

            if (f != null) {
                updateBucket(root, f, count);
            }
        }
    }

    private static void updateBucket(Bucket bucket, long code, Long count) {

        if (bucket == null) {
            return;
        }

        Bucket node = decode(code, bucket);

        if (node == null) {
            System.err.println("Bucket node with code " + code + " was not found.");
            return;
        }

        node.fraction = 0;

    System.out.println("oldcount: " + node.count + " correct is:" + count + " code:" + node.code);

        if (count != null) {
            node.count = count;
        } else {
            node.count = 0;
        }
    }

    public static Bucket decode(long code, Bucket node) {

        String binary = Long.toBinaryString(code);

        if (binary.charAt(0) != '1') {
            System.err.println("""
                Bucket is corrupted (missing sentinel bit).
                Please rebuild or update the table from scratch.
                """);
            return null;
        }

        if ((binary.length() % 2 == 0)) {
            System.err.println("""
                Bucket is corrupted (invalid code length).
                Please rebuild or update the index of table from scratch.
                """);
            return null;
        }

        for (int i = 1; i < binary.length(); i += 2) {

            String childBits = binary.substring(i, i + 2);

            node = switch (childBits) {
                case "00" -> node.topright;
                case "01" -> node.topleft;
                case "10" -> node.bottomright;
                case "11" -> node.bottomleft;
                default -> null;
            };

            if (node == null) {
                System.err.println("""
                    Bucket is corrupted (invalid bucket path).
                    Please rebuild or update index of the table from scratch.
                    """);
                return null;
            }
        }

        return node;
    }
}





