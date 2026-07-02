package ir.smh.spatialbricks.create_datasets;

import ir.smh.spatialbricks.config.SparkConfig;
import ir.smh.spatialbricks.core.SpatialWriting;
import ir.smh.spatialbricks.core.TableSpec;
import ir.smh.spatialbricks.encoder.converttogeometry.GeometryReader;
import ir.smh.spatialbricks.encoder.converttogeometry.GeoJsonGeometricalAdapter;
import ir.smh.spatialbricks.udf.FlattenSpatialParquet;
import ir.smh.spatialbricks.udf.SpatialParquet;
import ir.smh.spatialbricks.udf.WKBIndexedParquet;
import ir.smh.spatialbricks.utilities.PowerPlanUtil;
import org.apache.sedona.spark.SedonaContext;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;

import java.io.IOException;

public class CreateAuBuildings {

    public static void main(String[] args)
            throws NoSuchTableException, IOException, InterruptedException {

        PowerPlanUtil.setPowerPlan(PowerPlanUtil.SPARK_TEST);

        try {

            String folderpath = "../datasets/aubuildings";

            var spark = SparkConfig.createSession(folderpath);

            try {

                spark.sparkContext().setLogLevel("ERROR");

                SedonaContext.create(spark);

                GeometryReader<?> geoJsonFile = new GeoJsonGeometricalAdapter();

                SpatialWriting spatialWriting =
                        new SpatialWriting(spark, geoJsonFile, new SpatialParquet(spark));

                SpatialWriting flattenSpatialWriting =
                        new SpatialWriting(spark, geoJsonFile, new FlattenSpatialParquet(spark));

                SpatialWriting wkbWriting = new SpatialWriting(spark, geoJsonFile, new WKBIndexedParquet(spark));

                String path = "../datasets/aubuildings/AUBuildingsndjson.geojson";

                TableSpec silverIndexed =
                        new TableSpec("silverIndexed", "aubuildings", folderpath);

                TableSpec silverUnindexed =
                        new TableSpec("silverUnindexed", "aubuildings", folderpath);

                TableSpec flattenSilverUnindexed =
                        new TableSpec("flattenSilverUnindexed", "aubuildings", folderpath);

                TableSpec flattenSilverIndexed =
                        new TableSpec("flattenSilverIndexed", "aubuildings", folderpath);

                TableSpec wkbUnindexed =
                        new TableSpec("wkbUnindexed", "aubuildings", folderpath);

                TableSpec wkbIndexed =
                        new TableSpec("wkbIndexed", "aubuildings", folderpath);

                long startTime = System.currentTimeMillis();

//                spatialWriting.bronzeLayerBinary(bronze, path);

//                wkbWriting.silverLayerWithoutBboxIndexing(wkbUnindexed, path);

//                wkbWriting.silverLayerWithBboxIndexing(wkbIndexed, path, 150000L, 131072L);

//                spatialWriting.silverLayerWithoutBboxIndexing(silverUnindexed, path);

//                spatialWriting.silverLayerWithBboxIndexing(silverIndexed, path, 150000L, 131072L);

//                flattenSpatialWriting.silverLayerWithoutBboxIndexing(flattenSilverUnindexed, path);

//                flattenSpatialWriting.silverLayerWithBboxIndexing(flattenSilverIndexed, path, 150000L, 131072L);

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

