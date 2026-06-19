package ir.smh.spatialbricks.create_datasets;

import ir.smh.spatialbricks.SpatialWriting;
import ir.smh.spatialbricks.TableSpec;
import ir.smh.spatialbricks.config.SparkConfigLocal;
import ir.smh.spatialbricks.encoder.GeometryReader;
import ir.smh.spatialbricks.encoder.udf.FlattenSpatialParquet;
import ir.smh.spatialbricks.encoder.udf.SpatialParquet;
import ir.smh.spatialbricks.encoder.udf.UDFRegistry;
import ir.smh.spatialbricks.encoder.udf.converttogeometry.geoJsonGeometricalAdapter;
import org.apache.sedona.spark.SedonaContext;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;

import java.io.IOException;

public class CreateInternetAndVoiceCoverage {

    public static void main(String[] args) throws NoSuchTableException, IOException, InterruptedException {

        var spark = SparkConfigLocal.createSession("../datasets/internet_and_voice_coverage");

        spark.sparkContext().setLogLevel("ERROR");

        SedonaContext.create(spark);

        GeometryReader<?>  geoJsonFile= new geoJsonGeometricalAdapter();

        SpatialWriting spatialWriting = new SpatialWriting(spark,geoJsonFile, new SpatialParquet());

        SpatialWriting flattenSpatialWriting = new SpatialWriting(spark,geoJsonFile, new FlattenSpatialParquet()  );



        TableSpec silverIndexed = new TableSpec("silverIndexed", "internet_and_voice_coverage", "");
        TableSpec silverUnindexed = new TableSpec("silverUnindexed", "internet_and_voice_coverage", "");
        TableSpec flattenSilverIndexed = new TableSpec("flattenSilverIndexed", "internet_and_voice_coverage", "");
        TableSpec flattenSilverUnindexed = new TableSpec("FlattenSilverUnindexed", "internet_and_voice_coverage", "");

        String path = String.format("../datasets/internet_and_voice_coverage/voice-bronze-layer/F477_Voice_1412b.geojson");

        long startTime = System.currentTimeMillis();

//        spatialWriting.silverLayerWithBboxIndexing(silverIndexed, path, 30000L, 131072L);

//        spatialWriting.silverLayerWithoutBboxIndexing(silverUnindexed, path );

//        flattenSpatialWriting.silverLayerWithBboxIndexing(flattenSilverIndexed,path,  30000L, 131072L);
//
        flattenSpatialWriting.silverLayerWithoutBboxIndexing(flattenSilverUnindexed,path);

        Long duration = System.currentTimeMillis() - startTime;

        System.out.println("Time of writing: "+ duration);

        spark.stop();

        Thread.sleep(3000);
    }
}

