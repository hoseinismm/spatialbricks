package ir.smh.spatialbricks.create_datasets;

import ir.smh.spatialbricks.encoder.converttogeometry.geoJsonGeometricalAdapter;
import ir.smh.spatialbricks.utilities.PowerPlanUtil;
import ir.smh.spatialbricks.core.SpatialWriting;
import ir.smh.spatialbricks.core.TableSpec;
import ir.smh.spatialbricks.config.SparkConfigLocal;
import ir.smh.spatialbricks.encoder.converttogeometry.GeometryReader;
import ir.smh.spatialbricks.udf.FlattenSpatialParquet;
import ir.smh.spatialbricks.udf.SpatialParquet;
import org.apache.sedona.spark.SedonaContext;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;

import java.awt.*;
import java.io.IOException;

public class CreateInternetAndVoiceCoverage {

    public static void main(String[] args) throws NoSuchTableException, IOException, InterruptedException {

        PowerPlanUtil.setPowerPlan(PowerPlanUtil.SPARK_TEST);

        try {

            String folderpath = "../datasets/internet_and_voice_coverage";

        var spark = SparkConfigLocal.createSession(folderpath);

            try {

//        spark.sparkContext().setLogLevel("ERROR");

        spark.catalog().clearCache();

        SedonaContext.create(spark);

        GeometryReader<?>  geoJsonFile= new geoJsonGeometricalAdapter();

        SpatialWriting spatialWriting = new SpatialWriting(spark,geoJsonFile, new SpatialParquet());

        SpatialWriting flattenSpatialWriting = new SpatialWriting(spark,geoJsonFile, new FlattenSpatialParquet()  );

        String path = String.format("../datasets/internet_and_voice_coverage/F477_Voice_1412b.geojson");

        TableSpec silverUnindexed = new TableSpec("silverUnindexed", "internet_and_voice_coverage", folderpath);
        TableSpec silverIndexed = new TableSpec("silverIndexed", "internet_and_voice_coverage", folderpath);
        TableSpec flattenSilverUnindexed = new TableSpec("flattenSilverUnindexed", "internet_and_voice_coverage", folderpath);
        TableSpec flattenSilverIndexed = new TableSpec("flattenSilverIndexed", "internet_and_voice_coverage", folderpath);

        TableSpec bronze = new TableSpec("bronze", "internet_and_voice_coverage", "");

        long startTime = System.currentTimeMillis();

          spatialWriting.bronzeLayerBinary(bronze, path );

//        spatialWriting.silverLayerWithoutBboxIndexing(silverUnindexed, path );

//        spatialWriting.silverLayerWithBboxIndexing(silverIndexed, path, 68L, 32L);

//        flattenSpatialWriting.silverLayerWithoutBboxIndexing(flattenSilverUnindexed,path);

//        flattenSpatialWriting.silverLayerWithBboxIndexing(flattenSilverIndexed,path,  68L, 32L);

        Long duration = System.currentTimeMillis() - startTime;

        System.out.println("Time of writing: "+ duration);

            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                spark.catalog().clearCache();
                spark.stop();
            }

        } finally {
            PowerPlanUtil.setPowerPlan(PowerPlanUtil.BALANCED);
            Toolkit.getDefaultToolkit().beep();
        }
        Thread.sleep(3000);
    }
}

