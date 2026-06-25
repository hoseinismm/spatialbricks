package ir.smh.spatialbricks.create_datasets;

import ir.smh.spatialbricks.core.SpatialWriting;
import ir.smh.spatialbricks.core.TableSpec;
import ir.smh.spatialbricks.config.SparkConfigLocal;
import ir.smh.spatialbricks.encoder.converttogeometry.GeometryReader;
import ir.smh.spatialbricks.encoder.converttogeometry.geoJsonGeometricalAdapter;
import ir.smh.spatialbricks.encoder.udf.FlattenSpatialParquet;
import ir.smh.spatialbricks.encoder.udf.SpatialParquet;
import ir.smh.spatialbricks.utilities.PowerPlanUtil;
import org.apache.sedona.spark.SedonaContext;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;

import java.io.IOException;

public class CreateAuBuildings {

    public static void main(String[] args)
            throws NoSuchTableException, IOException, InterruptedException {

        PowerPlanUtil.setPowerPlan(PowerPlanUtil.SPARK_TEST);

        try {

            var spark = SparkConfigLocal.createSession("../datasets/aubuildings");

            try {

                spark.sparkContext().setLogLevel("ERROR");

                SedonaContext.create(spark);

                GeometryReader<?> geoJsonFile = new geoJsonGeometricalAdapter();

                SpatialWriting spatialWriting =
                        new SpatialWriting(spark, geoJsonFile, new SpatialParquet());

                SpatialWriting flattenSpatialWriting =
                        new SpatialWriting(spark, geoJsonFile, new FlattenSpatialParquet());

                String path = "../datasets/aubuildings/AUBuildingsndjson.geojson";

                TableSpec bronze =
                        new TableSpec("bronze", "aubuildings", "");

                TableSpec silverIndexed =
                        new TableSpec("silverIndexed", "aubuildings", "");

                TableSpec silverUnindexed =
                        new TableSpec("silverUnindexed", "aubuildings", "");

                TableSpec flattenSilverUnindexed =
                        new TableSpec("flattenSilverUnindexed", "aubuildings", "");

                TableSpec flattenSilverIndexed =
                        new TableSpec("flattenSilverIndexed", "aubuildings", "");

                long startTime = System.currentTimeMillis();

//                spatialWriting.bronzeLayerBinary(bronze, path);

//                spatialWriting.silverLayerWithoutBboxIndexing(silverUnindexed, path);

//                spatialWriting.silverLayerWithBboxIndexing(
//                        silverIndexed, path, 150000L, 131072L
//                );

//                flattenSpatialWriting.silverLayerWithoutBboxIndexing(flattenSilverUnindexed, path);

                flattenSpatialWriting.silverLayerWithBboxIndexing(
                        flattenSilverIndexed, path, 150000L, 131072L
                );

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

