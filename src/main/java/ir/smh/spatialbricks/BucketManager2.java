package ir.smh.spatialbricks;

import java.io.*;
import java.util.HashSet;
import java.util.Set;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class BucketManager2 {
    public static BucketCollection addGeosToBuckets(int[] newgeos, Bucket buckets, long MAX_SIZE) {
        Set<Integer> neededBucketMinsForNewRecords = new HashSet<>();
        BucketCollection result;

            for (int geo : newgeos) {
                //System.out.println("now: "+ geo  );
                try {
                    boolean valid = true;
                    Bucket current = buckets;
                    if (bucketOutOfRange(geo, current)) {
                        System.out.println("*********this geo is out of range: "+geo);
                        continue;
                    }
                    while (current.hasChildren) {
                        if (geo >= current.mid && geo < current.max) {
                            current = current.right;
                        } else if (geo < current.mid && geo >= current.min) {
                            current = current.left;
                        } else {
                            System.out.println("********a problem in your algorithm for:"+geo);
                            valid=false;
                            break;
                        }
                    }
                    if (!valid) continue;


                    if ((current.count >= MAX_SIZE) && (current.max - current.min >= 2)) {

                        current.left = new Bucket(current.min, current.mid, 0L, current);
                        current.right = new Bucket(current.mid, current.max, 0L, current);
                        current.hasChildren = true;
                        current.left.brother = current.right;
                        current.right.brother = current.left;
                        if (geo < current.mid) {
                            current.left.count++;
                            neededBucketMinsForNewRecords.add(current.min);
                        }
                        else {
                            current.right.count++;
                            neededBucketMinsForNewRecords.add(current.mid);
                        }
                    }  else {
                        current.count++;
                    }


                } catch (Exception e) {
                    System.err.println("Error processing this geo value: " + geo + " - " + e.getMessage());
                    e.printStackTrace();
                }
            }
            result=new BucketCollection(neededBucketMinsForNewRecords,buckets);

        return result;
    }


    public static Bucket removeGeosFromBuckets(int[] removedgeos, Bucket buckets, long MIN_SIZE) {
        try {
            for (int geo : removedgeos) {
                try {

                    boolean valid = true;
                    Bucket current = buckets;
                    if (bucketOutOfRange(geo, current)) {
                        System.out.println("*********this geo is out of range: "+geo);
                        continue;
                    }
                    while (current.hasChildren) {
                        if (geo >= current.mid && geo < current.max) {
                            current = current.right;
                        } else if (geo < current.mid && geo >= current.min) {
                            current = current.left;
                        } else {
                            System.out.println("********a problem in your algorithm for:"+geo);
                            valid=false;
                            break;

                        }
                    }
                    if (!valid) continue;

                    if (current.countOfEveryGeo[geo-current.min] > 0) {
                        current.countOfEveryGeo[geo-current.min]--;
                        current.count--;
                    }

                    while (current.parent != null && current.max-current.min <  1048576) {

                        if (current.count + current.brother.count < MIN_SIZE) {

                            int x= current.countOfEveryGeo.length;
                            current.parent.countOfEveryGeo=new int[x*2];
                            System.arraycopy(current.countOfEveryGeo, 0, current.parent.countOfEveryGeo, 0, x);
                            System.arraycopy(current.brother.countOfEveryGeo, 0, current.parent.countOfEveryGeo, x,x);
                            current.parent.count = current.count + current.brother.count;
                            current.countOfEveryGeo = null;
                            current.count = 0L;
                            current.brother.countOfEveryGeo = null;
                            current.brother.count = 0L;

                            current = current.parent;
                            current.hasChildren = false;
                            current.left = null;
                            current.right = null;

                        } else break;
                    }



                } catch (Exception e) {
                    System.err.println("Error processing this geo value for removal: " + geo + " - " + e.getMessage());
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
            System.err.println("Error in removeGeosToBuckets: " + e.getMessage());
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


    public static class Bucket implements Serializable {
        private static final long serialVersionUID = 1L;

        int min;
        int max;
        long count;
        int mid;
        Bucket brother;
        Bucket parent;
        Bucket right;
        Bucket left;
        int[] countOfEveryGeo;
        boolean hasChildren;

        Bucket(int min, int max, long count, Bucket parent) {
            this.parent = parent;
            this.min = min;
            this.max = max;
            this.count = count;
            this.mid = min + (max - min) / 2;
            this.hasChildren = false;

        }

        Bucket(int min, int max, long count) {

            this.min = min;
            this.max = max;
            this.count = count;
            this.mid = min + (max - min) / 2;
            this.hasChildren = false;

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


}


