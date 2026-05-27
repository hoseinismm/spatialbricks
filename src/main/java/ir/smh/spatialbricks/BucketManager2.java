package ir.smh.spatialbricks;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;

import java.io.*;
import java.util.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class BucketManager2 {
    public static Bucket addGeosToBuckets(int[] newgeos, Bucket buckets, long MAX_SIZE) {

        long effectiveMaxSize = (MAX_SIZE < 1) ? 1L : MAX_SIZE;

            try {

                for (int geo : newgeos) {

                    //System.out.println("now: "+ geo  );

                    Bucket current = buckets;
                    if (bucketOutOfRange(geo, current)) {
                        System.out.println("*********this geo is out of range: " + geo);
                        continue;
                    }

                    while (current.hasChildren) {
                        if (geo >= current.mid) {
                            current = current.right;
                        } else {
                            current = current.left;
                        }
                    }

                    if ((current.count >= effectiveMaxSize) && (current.max - current.min >= 2)) {

                        current.createChild();
                        current.hasChildren = true;

                        if (geo < current.mid) {
                            current = current.left;
                        } else {
                            current = current.right;
                        }
                    }
                    current.count++;
                }
            }
            catch (Exception e) {
                    System.err.println("Error processing geo values: "  + e.getMessage());
                    e.printStackTrace();
                }
        return buckets;
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


    Bucket(int min, int max, long count) {

        this.min = min;
        this.max = max;
        this.count = count; // رفرنس منتقل می‌شود
        this.mid = min + (max - min) / 2;
        this.hasChildren = false;

    }

    public void createChild() {
        this.left = new Bucket(this.min, this.mid, 0L);
        this.right = new Bucket(this.mid, this.max, 0L);
        this.hasChildren = true;
    }




    public void incrementCount() {
        this.count++;
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

    private static Bucket splitbucket(Bucket bucket) {
        if (bucket.max-bucket.min>1048576) {
            //System.out.println(bucket.min+"  "+bucket.max+" "+bucket.mid);
            bucket.left = new Bucket(bucket.min, bucket.mid, 0L);
            bucket.right = new Bucket(bucket.mid, bucket.max, 0L);
            bucket.hasChildren = true;
            splitbucket(bucket.left);
            splitbucket(bucket.right);
        }
        return bucket;
    }

    public static Bucket initialBucket() {

        Bucket initial = new Bucket(0, 1073741824, 0L);
        System.out.println(initial.min+"  "+initial.max);
        initial = splitbucket(initial);
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


    public static int[] computeBucketBorders(Dataset<Row> df, String bucketFile) { // اضافه کردن ورودی نام فایل
        List<Integer> list = df
                .select("geohash_numeric")
                .as(Encoders.INT())
                .collectAsList();

        int[] geos = list.stream().mapToInt(Integer::intValue).toArray();
        Bucket bucket;

        File f = new File(bucketFile);

        if (f.exists()) {
            bucket = loadBucket(bucketFile);
            System.out.println("Bucket loaded from: " + bucketFile);
            if (bucket == null) bucket = initialBucket();
        } else {
            bucket = initialBucket();
            System.out.println("Created new bucket for: " + bucketFile);
        }

        Bucket newBucketsAfterAddingGeos = addGeosToBuckets(geos, bucket, 512);

        List<Integer> borders = List.of(newBucketsAfterAddingGeos.max);

        List<Integer> ListOfBorders = extractMinBordersFromBucket(newBucketsAfterAddingGeos, borders);

        int[] array = ListOfBorders.stream()
                .mapToInt(i -> i)
                .toArray();

        saveBucket(newBucketsAfterAddingGeos, bucketFile);

        return  array;
    }


    public static void updateTreeFromStats(List<Row> rows, Bucket root) {
        for (Row row : rows) {
            // توجه: آیس‌برگ اعداد را در متادیتا ممکن است Long برگرداند
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


        if (node.min == floor && node.max == ceiling) {
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
        }
        return  borders;
    }
}


