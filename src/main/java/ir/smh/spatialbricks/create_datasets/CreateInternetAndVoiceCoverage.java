package ir.smh.spatialbricks.create_datasets;

import ir.smh.spatialbricks.SpatialWriting;
import ir.smh.spatialbricks.TableSpec;
import ir.smh.spatialbricks.config.SparkConfig;
import ir.smh.spatialbricks.config.SparkConfigLocal;
import ir.smh.spatialbricks.encoder.GeometryReader;
import ir.smh.spatialbricks.encoder.udf.converttogeometry.geoJsonGeometricalAdapter;
import ir.smh.spatialbricks.encoder.udf.converttogeometry.geoJsonReaderAdapter;
import org.apache.sedona.spark.SedonaContext;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;

import java.io.IOException;

public class CreateInternetAndVoiceCoverage {

    public static void main(String[] args) throws NoSuchTableException, IOException, InterruptedException {

        var spark = SparkConfig.createSession("../datasets/internet_and_voice_coverage");

        spark.sparkContext().setLogLevel("ERROR");

        SedonaContext.create(spark);

        GeometryReader<?>  geoJsonFile= new geoJsonGeometricalAdapter();

        SpatialWriting spatialWriting= new SpatialWriting(spark,geoJsonFile );

        TableSpec silver = new TableSpec("indexeddataset", "internet_and_voice_coverage", "");

        TableSpec silverunindexed = new TableSpec("unindexeddataset", "internet_and_voice_coverage", "");

        TableSpec bronze = new TableSpec("stringformat", "internet_and_voice_coverage", "");

        TableSpec bronze2 = new TableSpec("binaryformat", "internet_and_voice_coverage", "");



        String path = String.format("../datasets/internet_and_voice_coverage/voice-bronze-layer/F477_Voice_1412b.geojson");

        //spatialWriting.silverLayerWithBboxIndexing(silver, path, 150000L, 131072L);

        //spatialWriting.silverLayerWithoutBboxIndexing(silverunindexed, path );

        //spatialWriting.bronzeLayer(bronze,path);

        spatialWriting.bronzeLayerBinary(bronze2,path);

        spark.stop();

        Thread.sleep(3000);
    }
}

