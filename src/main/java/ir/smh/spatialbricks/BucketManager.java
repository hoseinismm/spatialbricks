package ir.smh.spatialbricks;

import ir.smh.spatialbricks.encoder.GeometryResult;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.*;
import java.util.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class BucketManager {
    public static void addGeosToBuckets(
            List<Row> rows,
            Bucket rootBucket,
            long maxSize,
            double fraction) {

        long sampleWeight = (long)Math.floor(1.0 / fraction);
        double foraddprecision = (1.0 / fraction) - sampleWeight;

        long effectiveMaxSize = (maxSize < 1) ? 1L : maxSize;

        try {

            for (Row row : rows) {

                double x = ((Number) row.getAs("x")).doubleValue();
                double y = ((Number) row.getAs("y")).doubleValue();

                int geo = GeometryResult.computeGeohashNumeric(x, y);

                Bucket current = rootBucket;

                if (bucketOutOfRange(geo, current)) {
                    System.out.println(
                            "*********this geo is out of range: "
                                    + geo);
                    continue;
                }
                while (current.hasChildren) {
                    if (geo >= current.mid) {
                        current = current.right;
                    } else {
                        current = current.left;
                    }
                }
                if ((current.count >= effectiveMaxSize)
                        && (current.max - current.min >= 2)) {

                    current.createChild();

                    if (geo < current.mid) {
                        current = current.left;
                    } else {
                        current = current.right;
                    }
                }
                current.count += sampleWeight;
                current.fraction+= foraddprecision;
                if (current.fraction >= 1) {
                    current.count++;
                    current.fraction -=1;
                }
            }
        } catch (Exception e) {
            System.err.println(
                    "Error processing geo values: "
                            + e.getMessage());
            e.printStackTrace();
        }
    }

public static class Bucket implements Serializable {
    private static final long serialVersionUID = 1L;

        int min;
        int max;
        int mid;
        long count;
        Bucket right;
        Bucket left;
        boolean hasChildren;
        double fraction;


    Bucket(int min, int max, long count) {

        this.min = min;
        this.max = max;
        this.count = count; // رفرنس منتقل می‌شود
        this.mid = min + (max - min) / 2;
        this.hasChildren = false;
        this.fraction = 0;
    }

    public void createChild() {
        this.left = new Bucket(this.min, this.mid, 0L);
        this.right = new Bucket(this.mid, this.max, 0L);
        this.hasChildren = true;
    }
}

    public static void saveBucket(Bucket bucket, String filename) {
        try (ObjectOutputStream out =
                     new ObjectOutputStream(
                             new GZIPOutputStream(
                                     new FileOutputStream(filename)))) {

            out.writeObject(bucket);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public static Bucket loadBucket(String filename) {
        try (ObjectInputStream in =
                     new ObjectInputStream(
                             new GZIPInputStream(
                                     new FileInputStream(filename)))) {

            return (Bucket) in.readObject();

        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    private static void splitbucket(Bucket bucket) {
        if (bucket.max-bucket.min>1048576) {
            //System.out.println(bucket.min+"  "+bucket.max+" "+bucket.mid);
            bucket.left = new Bucket(bucket.min, bucket.mid, 0L);
            bucket.right = new Bucket(bucket.mid, bucket.max, 0L);
            bucket.hasChildren = true;
            splitbucket(bucket.left);
            splitbucket(bucket.right);
        }
    }

    public static Bucket initialBucket() {

        Bucket initial = new Bucket(0, 1073741824, 0L);
        System.out.println(initial.min+"  "+initial.max);
        splitbucket(initial);
        System.out.println("bucketinitial created");
        return initial;

    }


    private  static long sumOfArrayElements (int[] x) {
        long sum = 0;
        for (int i: x) {
            sum += i;
        }
        return sum;
    }

    private static boolean bucketOutOfRange (int geo, Bucket bucket) {
        return geo < bucket.min || geo >= bucket.max;
    }


    public static List<Integer> computeBucketBorders(
            Dataset<Row> df,
            String bucketFile,
            long rowsCapableOfProcessingByDriver,
            long maxPartitionSize,
            Long totalRowsHint) {

        long totalRows =
                totalRowsHint != null
                        ? totalRowsHint
                        : df.count();

        double fraction =
                Math.min(1.0, (double) rowsCapableOfProcessingByDriver / totalRows);

        List<Row> rows = df
                .sample(false, fraction)
                .selectExpr(
                        "geometry.parts[0].coordinates[0].x as x",
                        "geometry.parts[0].coordinates[0].y as y"
                )
                .collectAsList();

        Bucket rootBucket;

        File f = new File(bucketFile);

        if (f.exists()) {
            rootBucket = loadBucket(bucketFile);
            System.out.println("Bucket loaded from: " + bucketFile);
            if (rootBucket == null) rootBucket = initialBucket();
        } else {
            rootBucket = initialBucket();
            System.out.println("Created new bucket for: " + bucketFile);
        }

        if (fraction != 0.0) {
            addGeosToBuckets(
                    rows,
                    rootBucket,
                    maxPartitionSize,
                    fraction
            );
        }

        List<Integer> borders = new ArrayList<>();

        List<Integer> listOfBorders =
                extractMinBordersFromBucket(
                        rootBucket,
                        borders
                );

        listOfBorders.add(rootBucket.max);

        saveBucket(rootBucket, bucketFile);

        return listOfBorders;
    }

    public static void updateTreeFromStats(List<Row> rows, Bucket root) {
        for (Row row : rows) {

            Integer f = row.getAs("floor_val");
            Integer c = row.getAs("ceiling_val");
            Long count = row.getAs("total_count");

            if (f != null && c != null) {
                updateBucket(root, f, c, count);
            }
        }
    }


    private static void updateBucket(Bucket node, int floor, int ceiling, long count) {
        if (node == null) return;
        node.fraction=0;
        if (node.min == floor && node.max == ceiling) {
            if (node.count>0 ) {
                System.out.println("oldcount: " + node.count + " correct is:" + count);
            }
            node.count=count;
            return;
        }

        if (ceiling <= node.mid) {
            updateBucket(node.left, floor, ceiling, count);
        } else if (floor >= node.mid) {
            updateBucket(node.right, floor, ceiling, count);
        }
    }

    public static List<Integer> extractMinBordersFromBucket(Bucket bucket, List<Integer> borders) {
        if (bucket.hasChildren) {
            extractMinBordersFromBucket(bucket.left,borders);
            extractMinBordersFromBucket(bucket.right,borders);
        }
        else {
            borders.add(bucket.min);
            if (bucket.count>0) {
                System.out.println(bucket.min+" : "+bucket.count);
            }
        }
        return  borders;
    }
}


