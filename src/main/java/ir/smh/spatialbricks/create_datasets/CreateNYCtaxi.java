package ir.smh.spatialbricks.create_datasets;

import ir.smh.spatialbricks.encoder.converttogeometry.GeometryReader;
import ir.smh.spatialbricks.encoder.converttogeometry.WKBReaderAdapter;
import ir.smh.spatialbricks.udf.WKBIndexedParquet;
import ir.smh.spatialbricks.utilities.PowerPlanUtil;
import ir.smh.spatialbricks.core.PipelineExecutor;
import ir.smh.spatialbricks.core.TableSpec;
import ir.smh.spatialbricks.config.SparkConfigLocal;
import ir.smh.spatialbricks.udf.FlattenSpatialParquet;
import ir.smh.spatialbricks.udf.SpatialParquet;
import org.apache.sedona.spark.SedonaContext;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;

import java.io.IOException;


public class CreateNYCtaxi {

    PipelineExecutor spatialWriting;

    public static void main(String[] args) throws NoSuchTableException, IOException, InterruptedException {
        PowerPlanUtil.setPowerPlan(PowerPlanUtil.SPARK_TEST);

        try {

            String folderpath ="../datasets/nyc_taxi";

            var spark = SparkConfigLocal.createSession(folderpath);
            try {

        spark.sparkContext().setLogLevel("ERROR");

        SedonaContext.create(spark);

        GeometryReader<?> geometryReader = new WKBReaderAdapter();

        PipelineExecutor spatialWriting= new PipelineExecutor(spark, null, new SpatialParquet(spark));

        PipelineExecutor flattenspatialwriting = new PipelineExecutor(spark, null, new FlattenSpatialParquet(spark));

        PipelineExecutor wkbWriting = new PipelineExecutor(spark, null, new WKBIndexedParquet(spark));


        TableSpec wkbUnindexed = new TableSpec("wkbUnindexed", "nyc_taxi", folderpath);
        TableSpec wkbIndexed = new TableSpec("wkbIndexed", "nyc_taxi", folderpath);
        TableSpec silverUnindexed = new TableSpec("silverUnindexed", "nyc_taxi", folderpath);
        TableSpec silverIndexed = new TableSpec("silverIndexed", "nyc_taxi", folderpath);
        TableSpec flattenSilverUnindexed = new TableSpec("flattenSilverUnindexed", "nyc_taxi", folderpath);
        TableSpec flattenSilverIndexed = new TableSpec("flattenSilverIndexed", "nyc_taxi",folderpath);
        TableSpec Sorted = new TableSpec("Sorted", "nyc_taxi","");
        TableSpec flattenSilverIndexedIncremental = new TableSpec("flattenSilverIndexedIncremental", "nyc_taxi",folderpath);

        String path =   "../datasets/nyc_taxi/yellow_tripdata_2009-0?.parquet";
        //String path= "../datasets/nyc_taxi/output_parts/part_*.parquet";

        long startTime = System.currentTimeMillis();

//        wkbWriting.customWriterWithoutBboxIndex(wkbUnindexed,path, "Start_Lon","Start_Lat");
//        wkbWriting.customWriterWithoutBboxIndex(wkbUnindexed,path, "End_Lon","End_Lat");
//
//        wkbWriting.customWriter(wkbIndexed,path,150000L, 131072L, "Start_Lon","Start_Lat");
//        wkbWriting.customWriter(wkbIndexed,path,150000L, 131072L, "End_Lon","End_Lat");


//        spatialWriting.customWriter(silverIndexed,path,150000L, 131072L, "Start_Lon","Start_Lat");
//        spatialWriting.customWriter(silverIndexed,path,150000L, 131072L, "End_Lon","End_Lat");

//        spatialWriting.customWriterWithoutBboxIndex(silverUnindexed,path, "Start_Lon","Start_Lat");
//        spatialWriting.customWriterWithoutBboxIndex(silverUnindexed,path, "End_Lon","End_Lat");

        flattenspatialwriting.xyToPintTableWithIndexing(flattenSilverIndexed,path,150000L, 131072L, "Start_Lon","Start_Lat");
        flattenspatialwriting.xyToPintTableWithIndexing(flattenSilverIndexed,path,150000L, 131072L, "End_Lon","End_Lat");
//                Table table = Spark3Util.loadIcebergTable(
//                        spark,
//                        "flattenSilverIndexed.nyc_taxi");
//                SparkActions
//                        .get(spark)
//                        .rewriteDataFiles(table)
//                        .execute();
//                SparkActions.get(spark)
//                        .expireSnapshots(table)
//                        .expireOlderThan(System.currentTimeMillis())
//                        .execute();
//                SparkActions.get(spark)
//                        .deleteOrphanFiles(table)
//                        .execute();

//        flattenspatialwriting.customWriterWithoutBboxIndex(flattenSilverUnindexed,path, "Start_Lon","Start_Lat");

//        flattenspatialwriting.customWriterWithoutBboxIndex(flattenSilverUnindexed,path, "End_Lon","End_Lat");

//        FlattenSpatialParquet flattenSpatialParquet = new FlattenSpatialParquet();
//        flattenSpatialParquet.registerAddGeohash(spark);

//        Dataset<Row> df=spark.read()
//                .format("iceberg")
//                .load("flattenSilverUnindexed.nyc_taxi")
//                .withColumn("geohash", expr("addgeohash(geometry)"))
//                .sort("geohash")
//                .drop("geohash");
//        IcebergTableCreator.createIcebergTableFromSchema(spark,df.schema(),Sorted.database(),Sorted.table());
//        String fullname=Sorted.database()+"."+Sorted.table();
//        df.writeTo(fullname).append();

//        AddOrUpdateBboxIndex addOrUpdateBboxIndex=new AddOrUpdateBboxIndex(spark,flattenSpatialParquet);
//        addOrUpdateBboxIndex.updateIndexing(flattenSilverUnindexed,150000L, 1048576L);

//        System.out.println("Press ENTER to exit...");
//        System.in.read();

//
//                for (int i = 0; i < 100; i++) {
//                    path = String.format(
//                            "../datasets/nyc_taxi/output_parts/part_%06d.parquet",
//                            i);
//
//                    System.out.println(path);
//
//                    flattenspatialwriting.customWriter(flattenSilverIndexedIncremental, path, 150000L, 131072L, "Start_Lon", "Start_Lat");
//                    flattenspatialwriting.customWriter(flattenSilverIndexedIncremental, path, 150000L, 131072L, "End_Lon", "End_Lat");
//
//                    if ((i + 1) % 20 == 0) {
//                        Table table = Spark3Util.loadIcebergTable(
//                                spark,
//                                "flattenSilverIndexedIncremental.nyc_taxi"
//                        );
//                        SparkActions
//                                .get(spark)
//                                .rewriteDataFiles(table)
//                                .execute();
//                        SparkActions.get(spark)
//                                .expireSnapshots(table)
//                                .expireOlderThan(System.currentTimeMillis())
//                                .execute();
//                        SparkActions.get(spark)
//                                .expireSnapshots(table)
//                                .expireOlderThan(System.currentTimeMillis())
//                                .execute();
//                    }
//                }

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


