package ir.smh.spatialbricks;

import ir.smh.spatialbricks.config.SparkConfig;
import ir.smh.spatialbricks.encoder.GeometryReader;
import ir.smh.spatialbricks.encoder.udf.converttogeometry.geoJsonGeometricalAdapter;
import org.apache.sedona.spark.SedonaContext;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;


import java.io.IOException;


public class Main {

    SpatialWriting spatialWriting;


    public static void main(String[] args) throws NoSuchTableException, IOException, InterruptedException {

        var spark = SparkConfig.createSession("../datasets/newyork");

        spark.sparkContext().setLogLevel("ERROR");

        SedonaContext.create(spark);

        //GeometryReader<?> adapter = new geoJsonGeometricalAdapter();

        SpatialWriting etl3= new SpatialWriting(spark);

        //TableSpec bronze = new TableSpec("bronzelayer", "FireStations", "");
        TableSpec silver = new TableSpec("silverlayer", "FireStations", "");

        etl3.customWriter( silver, "../datasets/newyork/raw-files/yellow_tripdata_2009-01.parquet", 25000, 1048576, "Start_lon", "Start_lat");

        //AddOrUpdateBboxIndex newindexjob= new AddOrUpdateBboxIndex(spark);

        //newindexjob.addIndexToUnindexedRows(silver, 100L, 512);


        //etl3.silverLayerWithoutIndex( silver, "../datasets/newyork/raw-files/group_id_1_ndjson.json", "bbox");
        //newindexjob.addIndexToUnindexedRows(silver, 100L, 512);
        //etl3.silverLayerWithoutIndex( silver, "../datasets/newyork/raw-files/group_id_2_ndjson.json", "bbox");
        //newindexjob.addIndexToUnindexedRows(silver, 100L, 512);
        //etl3.silverLayerWithoutIndex( silver, "../datasets/newyork/raw-files/group_id_3_ndjson.json", "bbox");

        //newindexjob.updateIndexing(silver, 100L, 512);

        /*spark.sql("""
                            CALL spark_catalog.system.expire_snapshots(
                                table => 'silverlayer.FireStations',
                                older_than => TIMESTAMP '2100-01-01 00:00:00',
                                retain_last => 1
                            )
            """);

                    //spark.sql("""
            SELECT *
            FROM silverlayer.FireStations.snapshots
            """).show(false);
            */


        spark.stop();
        Thread.sleep(3000);
    }
}

