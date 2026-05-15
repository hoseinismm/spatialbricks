package ir.smh.spatialbricks;

import java.io.*;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class BucketManager {
    public static BucketCollection addGeosToBuckets(int[] newgeos, Bucket buckets, long MAX_SIZE) {
        List<Integer> oldBordersForOldRecords = new ArrayList<>();
        List<Integer> updatesForOldRecords = new ArrayList<>();
        List<Integer> neededBucketMinsForNewRecords = new ArrayList<>();
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
                    current.count++;
                    if (current.countOfEveryGeo == null) {
                        current.countOfEveryGeo = new int[current.max-current.min];
                    }
                    current.countOfEveryGeo[geo - current.min]++;

                    while ((current.count > MAX_SIZE) && (current.max - current.min >= 2)) {

                        current.left = new Bucket(current.min, current.mid, 0L, current);
                        current.right = new Bucket(current.mid, current.max, 0L, current);
                        current.hasChildren=true;
                        current.left.brother = current.right;
                        current.right.brother = current.left;

                        int x= current.mid - current.min;
                        int y= current.max - current.mid;

                        current.left.countOfEveryGeo=new int[x];
                        current.right.countOfEveryGeo=new int[y];

                        updatesForOldRecords.add(current.left.min);
                        updatesForOldRecords.add(current.right.min);
                        oldBordersForOldRecords.add(current.min);

                        System.arraycopy(current.countOfEveryGeo,0,current.left.countOfEveryGeo,0,x);
                        System.arraycopy(current.countOfEveryGeo,x,current.right.countOfEveryGeo,0,x);

                        current.left.count= sumOfArrayElements(current.left.countOfEveryGeo);
                        current.right.count= sumOfArrayElements(current.right.countOfEveryGeo);

                        current.countOfEveryGeo = null;
                        current.count = 0L;

                        if (current.left.count > MAX_SIZE) {
                            current = current.left;
                        } else if (current.right.count > MAX_SIZE) {
                            current = current.right;
                        } else  {

                            break;
                        }


                    }

                    // تعیین باکت موردنیاز برای geo فعلی
                    if (current.count!=0) {
                        neededBucketMinsForNewRecords.add(current.min);
                    } else if (geo<current.mid) {
                        neededBucketMinsForNewRecords.add(current.right.min);
                    } else neededBucketMinsForNewRecords.add(current.left.min);

                } catch (Exception e) {
                    System.err.println("Error processing this geo value: " + geo + " - " + e.getMessage());
                    e.printStackTrace();
                }
            }

            // حذف عناصر تکراری داخل لیست ها با تبدیل آنها به set و برگرداندن به لیست
            neededBucketMinsForNewRecords = new ArrayList<>(new LinkedHashSet<>(neededBucketMinsForNewRecords));
            updatesForOldRecords = new ArrayList<>(new LinkedHashSet<>(updatesForOldRecords));
            oldBordersForOldRecords = new ArrayList<>(new LinkedHashSet<>(oldBordersForOldRecords));

            result=new BucketCollection(oldBordersForOldRecords,updatesForOldRecords,neededBucketMinsForNewRecords,buckets);

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

    // تجمیع باکت با باکتهای به روزرسانی شده در لیست مجزا برای به روز رسانی جدول قبلی
    public static class BucketCollection implements Serializable {

        private List<Integer> oldBordersForOldRecords;
        private List<Integer> updatesForOldRecords;
        private List<Integer> neededBucketMinsForNewRecords;
        private List<Integer> updates;
        private Bucket bucket;

        BucketCollection(List<Integer> oldBordersForOldRecords, List<Integer> updatesForOldRecords, List<Integer> neededBucketMinsForNewRecords,  Bucket bucket) {
            this.oldBordersForOldRecords=oldBordersForOldRecords;
            this.updatesForOldRecords=updatesForOldRecords;
            this.neededBucketMinsForNewRecords=neededBucketMinsForNewRecords;
            this.bucket = bucket;
        }

        public BucketCollection getBucketCollection() {
            return this;
        }

        public List<Integer> getupdatesForOldRecords() {
            return updatesForOldRecords;
        }

        public List<Integer> getneededBucketMinsForNewRecords() {
            return neededBucketMinsForNewRecords;
        }

        public List<Integer> getoldBordersForOldRecords() {
            return oldBordersForOldRecords;
        }


        public Bucket getBucket() {
            return bucket;
        }
        public void setBucket(Bucket bucket) {
            this.bucket = bucket;
        }
        public void setUpdates(int min) {
            updates.add(min);
        }
        public void clearUpdates() {
            updates.clear();
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


