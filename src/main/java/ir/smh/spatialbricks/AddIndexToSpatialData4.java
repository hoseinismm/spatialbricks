package ir.smh.spatialbricks;

import ir.smh.spatialbricks.encoder.GeometryOptions;
import ir.smh.spatialbricks.encoder.GeometryReader;
import ir.smh.spatialbricks.encoder.udf.SparkUdfs;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;

import java.io.File;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;


import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;


public class AddIndexToSpatialData4 implements Serializable {
    private final SparkSession spark;
    private final GeometryOptions options;
    private final GeometryReader<?> adapter;

    public AddIndexToSpatialData4(SparkSession spark, GeometryOptions options, GeometryReader<?> adapter) {
        this.spark = spark;
        this.options = options;
        this.adapter = adapter;
    }

    public void AddorUpdateIndex(TableSpec silver) throws NoSuchTableException {

        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());

        String fullName = silver.database() + "." + silver.table();

        String bucketFileName = "bucket_" + silver.database() + "_" + silver.table() + ".gz";

        Dataset<Row> table =
                spark.read()
                        .format("iceberg")
                        .load(fullName);

        spark.sql("""          
                SELECT count(*) AS silvercount1
                FROM spark_catalog.silverlayer.FireStations
         """).show();


        Dataset<Row> notIndexedRows = table.filter(col("bucket_min").equalTo(-1));


        int[] neededBucketMinsForNewRecords = BucketManager2.computeBucketBorders(notIndexedRows, bucketFileName);


        Broadcast<int[]> broadcastBorders2 = jsc.broadcast(neededBucketMinsForNewRecords);

        SparkUdfs.registerFindFloorUdf(spark, broadcastBorders2);

        notIndexedRows = notIndexedRows.withColumn(
                "bucket_min",
                callUDF("findFloor", col("geohash_numeric"))
        );

        notIndexedRows.writeTo("catalog.db.table_name")
                .overwrite(functions.col("bucket_min").equalTo(-1));


        spark.sql("""          
                SELECT count(*) AS silvercount4
                FROM spark_catalog.silverlayer.FireStations
         """).show();

        long count = spark.sql("""
        SELECT count(*) AS cnt
        FROM silverlayer.FireStations
        WHERE bucket_min = -1
        """).first().getLong(0);

        System.out.println("count = " + count);
        System.out.println("notIndexedRows count = " + notIndexedRows.count());

        System.out.println("newNeededBucketMinsForNewRecords = " + Arrays.toString(neededBucketMinsForNewRecords));

        return;
    }

}
