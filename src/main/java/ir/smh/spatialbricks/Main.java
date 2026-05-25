package ir.smh.spatialbricks;

import ir.smh.spatialbricks.config.SparkConfig;
import ir.smh.spatialbricks.encoder.GeometryOptions;
import ir.smh.spatialbricks.encoder.GeometryReader;
import ir.smh.spatialbricks.encoder.udf.converttogeometry.geoJsonGeometricalAdapter;
import org.apache.sedona.spark.SedonaContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.opengis.filter.expression.Add;

import java.io.IOException;


public class Main {

    public static void main(String[] args) throws NoSuchTableException, IOException {

        var spark = SparkConfig.createSession("../datasets/newyork");

        spark.sparkContext().setLogLevel("ERROR");

        SedonaContext.create(spark);

        GeometryOptions options = GeometryOptions.of("geohash","center");

        GeometryReader<?> adapter = new geoJsonGeometricalAdapter();

        //SpatialETL etl = new SpatialETL(spark, options, adapter);

        SpatialWritingWithoutIndex etl3= new SpatialWritingWithoutIndex(spark, options, adapter);


        TableSpec bronze = new TableSpec("bronzelayer", "FireStations", "");
        TableSpec silver = new TableSpec("silverlayer", "FireStations", "");

        etl3.processFile(bronze, silver, "../datasets/newyork/raw-files/group_id_0_ndjson.json");
        AddindexToSpatialData3 etl4 = new AddindexToSpatialData3(spark);

        etl4.AddOrUpdateIndex(silver,"Geometry.center.x","Geometry.center.y");

        //etl2.processFile(bronze, silver, "../datasets/newyork/raw-files/group_id_1_ndjson.json");
        //etl2.processFile(bronze, silver, "../datasets/newyork/raw-files/group_id_2_ndjson.json");
        //etl2.processFile(bronze, silver, "../datasets/newyork/raw-files/group_id_3_ndjson.json");


        spark.stop();
    }
}
