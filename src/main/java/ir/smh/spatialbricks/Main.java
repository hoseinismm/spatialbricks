package ir.smh.spatialbricks;

import ir.smh.spatialbricks.config.SparkConfig;
import ir.smh.spatialbricks.encoder.GeometryOptions;
import ir.smh.spatialbricks.encoder.GeometryReader;
import ir.smh.spatialbricks.encoder.udf.converttogeometry.geoJsonGeometricalAdapter;
import org.apache.sedona.spark.SedonaContext;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;

import java.io.IOException;


public class Main {

    SpatialWriting spatialWriting;


    public static void main(String[] args) throws NoSuchTableException, IOException {

        var spark = SparkConfig.createSession("../datasets/newyork");

        spark.sparkContext().setLogLevel("ERROR");

        SedonaContext.create(spark);

        GeometryReader<?> adapter = new geoJsonGeometricalAdapter();

        //SpatialETL etl = new SpatialETL(spark, options, adapter);

        SpatialWriting etl3= new SpatialWriting(spark, adapter);

        TableSpec bronze = new TableSpec("bronzelayer", "FireStations", "");
        TableSpec silver = new TableSpec("silverlayer", "FireStations", "");

        etl3.silverLayerWithoutIndex( silver, "../datasets/newyork/raw-files/group_id_0_ndjson.json");

        SpatialIndexBackfillJob spatialIndexBackfillJob=new SpatialIndexBackfillJob(spark);
        spatialIndexBackfillJob.computeGeohashForUnindexedRows(silver);
        spatialIndexBackfillJob.execute(silver);





        //etl2.processFile(bronze, silver, "../datasets/newyork/raw-files/group_id_1_ndjson.json");
        //etl2.processFile(bronze, silver, "../datasets/newyork/raw-files/group_id_2_ndjson.json");
        //etl2.processFile(bronze, silver, "../datasets/newyork/raw-files/group_id_3_ndjson.json");


        spark.stop();
    }
}
