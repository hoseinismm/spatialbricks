package ir.smh.spatialbricks.create_datasets;

import ir.smh.spatialbricks.encoder.converttogeometry.GeoJsonGeometricalAdapter;
import ir.smh.spatialbricks.udf.WKBIndexedParquet;
import ir.smh.spatialbricks.utilities.PowerPlanUtil;
import ir.smh.spatialbricks.core.PipelineExecutor;
import ir.smh.spatialbricks.core.TableSpec;
import ir.smh.spatialbricks.config.SparkConfigLocal;
import ir.smh.spatialbricks.encoder.converttogeometry.GeometryReader;
import ir.smh.spatialbricks.udf.FlattenSpatialParquet;
import ir.smh.spatialbricks.udf.SpatialParquet;
import org.apache.sedona.spark.SedonaContext;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;

import java.io.IOException;

public class CreateMsBuildings {

    public static void main(String[] args)
            throws NoSuchTableException, IOException, InterruptedException {

        PowerPlanUtil.setPowerPlan(PowerPlanUtil.SPARK_TEST);

        try {

            String folderpath = "../datasets/msbuildings";

            var spark = SparkConfigLocal.createSession(folderpath);


            try {

                spark.sparkContext().setLogLevel("ERROR");

                SedonaContext.create(spark);

                GeometryReader<?> geoJsonFile = new GeoJsonGeometricalAdapter();

                PipelineExecutor wkbWriting =
                        new PipelineExecutor(spark, geoJsonFile, new WKBIndexedParquet(spark));

                PipelineExecutor spatialWriting =
                        new PipelineExecutor(spark, geoJsonFile, new SpatialParquet(spark));

                PipelineExecutor flattenSpatialWriting =
                        new PipelineExecutor(spark, geoJsonFile, new FlattenSpatialParquet(spark));

                String path = "../datasets/msbuildings/MSBuildingsndjson.geojson";

                TableSpec wkbUnindexed =
                        new TableSpec("wkbUnindexed", "msbuildings", folderpath);

                TableSpec wkbIndexed =
                        new TableSpec("wkbIndexed", "msbuildings", folderpath);

                TableSpec silverIndexed =
                        new TableSpec("silverIndexed", "msbuildings", folderpath);

                TableSpec silverUnindexed =
                        new TableSpec("silverUnindexed", "msbuildings", folderpath);

                TableSpec flattenSilverUnindexed =
                        new TableSpec("flattenSilverUnindexed", "msbuildings", folderpath);

                TableSpec flattenSilverIndexed =
                        new TableSpec("flattenSilverIndexed", "msbuildings", folderpath);

                long startTime = System.currentTimeMillis();

//                wkbWriting.silverLayerWithoutBboxIndexing(wkbUnindexed, path);

//                wkbWriting.silverLayerWithBboxIndexing(wkbIndexed, path, 150000L, 1048576L);

//                spatialWriting.silverLayerWithoutBboxIndexing(silverUnindexed, path);

//                spatialWriting.silverLayerWithBboxIndexing(silverIndexed, path, 150000L, 1048576L);

//                flattenSpatialWriting.silverLayerWithoutBboxIndexing(flattenSilverUnindexed, path);

                flattenSpatialWriting.AddDataWithIndexing(flattenSilverIndexed, path, 150000L, 131072L);

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

