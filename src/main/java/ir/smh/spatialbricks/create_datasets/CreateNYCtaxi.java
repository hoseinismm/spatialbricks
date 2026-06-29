package ir.smh.spatialbricks.create_datasets;

import ir.smh.spatialbricks.utilities.PowerPlanUtil;
import ir.smh.spatialbricks.core.SpatialWriting;
import ir.smh.spatialbricks.core.TableSpec;
import ir.smh.spatialbricks.config.SparkConfigLocal;
import ir.smh.spatialbricks.udf.FlattenSpatialParquet;
import ir.smh.spatialbricks.udf.SpatialParquet;
import org.apache.sedona.spark.SedonaContext;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;

import java.io.IOException;


public class CreateNYCtaxi {

    SpatialWriting spatialWriting;

    public static void main(String[] args) throws NoSuchTableException, IOException, InterruptedException {
        PowerPlanUtil.setPowerPlan(PowerPlanUtil.SPARK_TEST);

        try {

            String folderpath ="../datasets/nyc_taxi";

            var spark = SparkConfigLocal.createSession(folderpath);
            try {

        spark.sparkContext().setLogLevel("ERROR");

        SedonaContext.create(spark);

        SpatialWriting spatialWriting= new SpatialWriting(spark, null, new SpatialParquet());

        SpatialWriting flattenspatialwriting = new SpatialWriting(spark, null, new FlattenSpatialParquet());

        TableSpec silverIndexed = new TableSpec("silverIndexed", "nyc_taxi", folderpath);
        TableSpec silverUnindexed = new TableSpec("silverUnindexed", "nyc_taxi", folderpath);
        TableSpec flattenSilverIndexed = new TableSpec("flattenSilverIndexed", "nyc_taxi", folderpath);
        TableSpec flattenSilverUnindexed = new TableSpec("flattenSilverUnindexed", "nyc_taxi",folderpath);
        TableSpec Sorted = new TableSpec("Sorted", "nyc_taxi","");

        String path =   "../datasets/nyc_taxi/yellow_tripdata_2009-0?.parquet";

        long startTime = System.currentTimeMillis();

//        spatialWriting.customWriter(silverIndexed,
//                path,150000L, 131072L, "Start_Lon","Start_Lat"
//        );
//        spatialWriting.customWriter(silverIndexed,
//                path,150000L, 131072L, "End_Lon","End_Lat"
//        );

//        spatialWriting.customWriterWithoutBboxIndex(silverUnindexed,
//                path, "Start_Lon","Start_Lat"
//        );
//        spatialWriting.customWriterWithoutBboxIndex(silverUnindexed,
//                path, "End_Lon","End_Lat"
//        );
//
//        flattenspatialwriting.customWriter(flattenSilverIndexed,
//                path,150000L, 131072L, "Start_Lon","Start_Lat"
//        );
//
//        flattenspatialwriting.customWriter(flattenSilverIndexed,
//                path,150000L, 131072L, "End_Lon","End_Lat"
//        );

//        flattenspatialwriting.customWriterWithoutBboxIndex(flattenSilverUnindexed,
//                path, "Start_Lon","Start_Lat"
//        );
////
//        flattenspatialwriting.customWriterWithoutBboxIndex(flattenSilverUnindexed,
//                path, "End_Lon","End_Lat"
//        );

//        FlattenSpatialParquet flattenSpatialParquet = new FlattenSpatialParquet();
//        flattenSpatialParquet.registerAddGeohash(spark);
//
//        Dataset<Row> df=spark.read()
//                .format("iceberg")
//                .load("flattenSilverUnindexed.nyc_taxi")
//                .withColumn("geohash", expr("addgeohash(geometry)"))
//                .sort("geohash")
//                .drop("geohash");////
//        IcebergTableCreator.createIcebergTableFromSchema(spark,df.schema(),Sorted.database(),Sorted.table());
//        String fullname=Sorted.database()+"."+Sorted.table();
//        df.writeTo(fullname).append();
////
//        AddOrUpdateBboxIndex addOrUpdateBboxIndex=new AddOrUpdateBboxIndex(spark,flattenSpatialParquet);
//        addOrUpdateBboxIndex.updateIndexing(flattenSilverUnindexed,150000L, 1048576L);

//        System.out.println("Press ENTER to exit...");
//        System.in.read();

                long duration = System.currentTimeMillis() - startTime;

                System.out.println("Time of writing : " + duration);

            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                spark.stop();
            }

        } finally {
            PowerPlanUtil.setPowerPlan(PowerPlanUtil.BALANCED);
        }

        Thread.sleep(3000);
    }
}


