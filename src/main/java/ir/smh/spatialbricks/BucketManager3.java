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

public class BucketManager3 {
    public static BucketCollection addGeosToBuckets(int[] newgeos, Bucket buckets, long MAX_SIZE, double multiplicationOfIncreasingSize) {

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

                    if (current.capacity==null) {
                        current.capacity=effectiveMaxSize;
                    }

                    if ((current.count.value >= current.capacity) && (current.max - current.min >= 2)) {

                        current.createRightChild();
                        current.hasRightChildren = true;
                        current.capacity=Math.round(current.capacity * multiplicationOfIncreasingSize);

                        if (geo > current.mid)  {
                            current = current.right;
                            } else {
                            current.createLeftChild();
                            current.hasLeftChildren = true;
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
    int borderOfchild;
    Long capacity;
    Counter count;
    Bucket parent;
    Bucket right;
    Bucket left;
    boolean hasLeftChildren;
    boolean hasRightChildren;

    Bucket(int min, int max, Counter count, Bucket parent) {
        this.parent = parent;
        this.min = min;
        this.max = max;
        this.count = count; // رفرنس منتقل می‌شود
        this.borderOfchild = (int)(min + (max - min) / fractionOfChild);
        this.hasLeftChildren = false;
        this.hasRightChildren = false;
    }


    public void createLeftChild() {
        this.left = new Bucket(this.min, this.mid, this.count, this);
        this.hasLeftChildren = true;
    }

    public void createRightChild() {
        this.right = new Bucket(this.mid, this.max, new Counter(0L), this);
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
        BucketManager3.Bucket bucket;

        File f = new File(bucketFile);

        if (f.exists()) {
            bucket = BucketManager3.loadBucket(bucketFile);
            System.out.println("Bucket loaded from: " + bucketFile);
            if (bucket == null) bucket = BucketManager3.initialBucket();
        } else {
            bucket = BucketManager3.initialBucket();
            System.out.println("Created new bucket for: " + bucketFile);
        }

        BucketManager3.BucketCollection result = BucketManager3.addGeosToBuckets(geos, bucket, 512);
        bucket=result.getBucket();


        int[] neededBucketMinsForNewRecords=result.getneededBucketMinsForNewRecords().stream().mapToInt(Integer::intValue).toArray();

        BucketManager3.saveBucket(bucket, bucketFile);
        Arrays.sort(neededBucketMinsForNewRecords);


        return  neededBucketMinsForNewRecords;
    }


}


