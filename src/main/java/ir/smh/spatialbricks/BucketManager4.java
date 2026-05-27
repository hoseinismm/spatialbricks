package ir.smh.spatialbricks;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;

import java.io.*;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class BucketManager4 {
    public static BucketCollection addGeosToBuckets(int[] newgeos, Bucket buckets, long MAX_SIZE) {

        long effectiveMaxSize = (MAX_SIZE < 1) ? 1L : MAX_SIZE;

        Set<Integer> neededBucketMinsForNewRecords = new HashSet<>();

        BucketCollection result;
        Integer geohash=null;

            try {

                for (int geo : newgeos) {
                    geohash=geo;
                    //System.out.println("now: "+ geo  );

                    boolean valid = true;
                    Bucket current = buckets;
                    if (bucketOutOfRange(geo, current)) {
                        System.out.println("*********this geo is out of range: " + geo);
                        continue;
                    }

                    while (true) {
                        if (geo >= current.mid && geo < current.max) {
                            if (current.hasRightChildren) {
                                current = current.right;
                            } else {
                                break;
                            }

                        } else if (geo < current.mid && geo >= current.min) {
                            if (current.hasLeftChildren) {
                                current = current.left;
                            } else {
                                break;
                            }
                        }
                    }

                    if (!valid) continue;



                    if ((current.count.value >= effectiveMaxSize) && (current.max - current.min >= 2)) {

                        if (geo < current.mid) {
                            current.createLeftChild(MAX_SIZE);
                            current.hasLeftChildren = true;
                            current = current.left;
                        } else {
                            current.createRightChild(MAX_SIZE);

                            current.hasRightChildren = true;
                            current = current.right;

                            break;
                        }
                    }
                    current.count.value++;
                    neededBucketMinsForNewRecords.add(current.min);
                }
            }
            catch (Exception e) {
                    System.err.println("Error processing this geo value: " + geohash + " - " + e.getMessage());
                    e.printStackTrace();
                }

            result=new BucketCollection(neededBucketMinsForNewRecords,buckets);

        return result;
    }


    public static Bucket removeGeosFromBuckets(int removedgeos, Bucket buckets, int bucket_min) {

                try {




                } catch (Exception e) {
                    System.err.println("Error processing this geo value for removal: " + geo + " - " + e.getMessage());
                    e.printStackTrace();
                }


        return buckets;
    }


    public static class BucketCollection implements Serializable {


        private Set<Integer> neededBucketMinsForNewRecords;

        private Bucket bucket;

        BucketCollection(Set<Integer> neededBucketMinsForNewRecords,  Bucket bucket) {

            this.neededBucketMinsForNewRecords=neededBucketMinsForNewRecords;
            this.bucket = bucket;
        }

        public BucketCollection getBucketCollection() {
            return this;
        }

        public Set<Integer> getneededBucketMinsForNewRecords() {
            return neededBucketMinsForNewRecords;
        }

        public Bucket getBucket() {
            return bucket;
        }

        public void setBucket(Bucket bucket) {
            this.bucket = bucket;
        }

    }

public static class Counter implements Serializable {
    public long value;
    public Counter(long value) { this.value = value; }
}


public static class Bucket implements Serializable {
    private static final long serialVersionUID = 1L;

    int min;
    int max;
    int mid;
    long capacity=65536L;
    Counter count;
    Bucket brother;
    Bucket parent;
    Bucket right;
    Bucket left;
    boolean hasLeftChildren;
    boolean hasRightChildren;

    Bucket(int min, int max, Counter count, Long capacity, Bucket parent,) {
        this.parent = parent;
        this.min = min;
        this.max = max;
        this.count = count; // رفرنس منتقل می‌شود
        this.mid = min + (max - min) / 2;
        this.hasLeftChildren = false;
        this.hasRightChildren = false;
        this.capacity = capacity;
    }


    public void createLeftChild(Long capacity) {
        this.left = new Bucket(this.min, this.mid, this.count,capacity, this);
        this.hasLeftChildren = true;
    }

    public void createRightChild(Long capacity) {
        this.right = new Bucket(this.mid, this.max, new Counter(0L),capacity, this);
        this.hasRightChildren = true;
    }


    public void incrementCount() {
        this.count.value++;
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
            bucket.left = new Bucket(bucket.min, bucket.mid, 0L, bucket);

            bucket.right = new Bucket(bucket.mid, bucket.max, 0L, bucket);
            bucket.hasChildren=true;
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
        BucketManager4.Bucket bucket;

        File f = new File(bucketFile);

        if (f.exists()) {
            bucket = BucketManager4.loadBucket(bucketFile);
            System.out.println("Bucket loaded from: " + bucketFile);
            if (bucket == null) bucket = BucketManager4.initialBucket();
        } else {
            bucket = BucketManager4.initialBucket();
            System.out.println("Created new bucket for: " + bucketFile);
        }

        BucketManager4.BucketCollection result = BucketManager4.addGeosToBuckets(geos, bucket, 512);
        bucket=result.getBucket();


        int[] neededBucketMinsForNewRecords=result.getneededBucketMinsForNewRecords().stream().mapToInt(Integer::intValue).toArray();

        BucketManager4.saveBucket(bucket, bucketFile);
        Arrays.sort(neededBucketMinsForNewRecords);


        return  neededBucketMinsForNewRecords;
    }


}


