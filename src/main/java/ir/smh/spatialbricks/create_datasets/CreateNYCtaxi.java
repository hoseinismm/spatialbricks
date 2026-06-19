package ir.smh.spatialbricks.create_datasets;

import ir.smh.spatialbricks.SpatialWriting;
import ir.smh.spatialbricks.TableSpec;
import ir.smh.spatialbricks.config.SparkConfig;
import ir.smh.spatialbricks.encoder.udf.FlattenSpatialParquet;
import ir.smh.spatialbricks.encoder.udf.SpatialParquet;
import org.apache.sedona.spark.SedonaContext;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;

import java.io.IOException;


public class CreateNYCtaxi {

    SpatialWriting spatialWriting;

    public static void main(String[] args) throws NoSuchTableException, IOException, InterruptedException {

        var spark = SparkConfig.createSession("../datasets/nyc_taxi");

        spark.sparkContext().setLogLevel("ERROR");

        SedonaContext.create(spark);

        SpatialWriting spatialWriting= new SpatialWriting(spark, null, new SpatialParquet());

        SpatialWriting flattenspatialwriting = new SpatialWriting(spark, null, new FlattenSpatialParquet());

        TableSpec silverIndexed = new TableSpec("silverIndexed", "nyc_taxi", "");
        TableSpec silverUnindexed = new TableSpec("silverUnindexed", "nyc_taxi", "");
        TableSpec flattenSilverIndexed = new TableSpec("flattenSilverIndexed", "nyc_taxi", "");
        TableSpec flattenSilverUnindexed = new TableSpec("FlattenSilverUnindexed", "nyc_taxi","");

        String path =   "../datasets/nyc_taxi/yellow_tripdata_2009-04.parquet";

        long startTime = System.currentTimeMillis();

//        spatialWriting.customWriter(silverIndexed,
//                path,150000L, 131072L, "Start_Lon","Start_Lat"
//        );
//        spatialWriting.customWriter(silverIndexed,
//                path,150000L, 131072L, "End_Lon","End_Lat"
//        );

//        spatialWriting.customWriterWithoutBboxIndex(silverUnindexed,
//                path, "Start_Lon","Start_Lat"
//        );
//        spatialWriting.customWriterWithoutBboxIndex(silverUnindexed,
//                path, "End_Lon","End_Lat"
//        );
//
//        flattenspatialwriting.customWriter(flattenSilverIndexed,
//                path,150000L, 131072L, "Start_Lon","Start_Lat"
//        );
//
//        flattenspatialwriting.customWriter(flattenSilverIndexed,
//                path,150000L, 131072L, "End_Lon","End_Lat"
//        );
//
        flattenspatialwriting.customWriterWithoutBboxIndex(flattenSilverUnindexed,
                path, "Start_Lon","Start_Lat"
        );

        flattenspatialwriting.customWriterWithoutBboxIndex(flattenSilverUnindexed,
                path, "End_Lon","End_Lat"
        );

        long duration = System.currentTimeMillis() - startTime;
        System.out.println("Writing duration: " + duration + "ms");

        spark.stop();
        Thread.sleep(3000);
    }
}

