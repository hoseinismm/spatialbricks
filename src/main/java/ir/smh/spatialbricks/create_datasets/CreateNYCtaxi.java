package ir.smh.spatialbricks.create_datasets;

import ir.smh.spatialbricks.SpatialWriting;
import ir.smh.spatialbricks.TableSpec;
import ir.smh.spatialbricks.config.SparkConfig;
import org.apache.sedona.spark.SedonaContext;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;

import java.io.IOException;


public class CreateNYCtaxi {

    SpatialWriting spatialWriting;

    public static void main(String[] args) throws NoSuchTableException, IOException, InterruptedException {

        var spark = SparkConfig.createSession("../datasets/nyc_taxi");

        spark.sparkContext().setLogLevel("ERROR");

        SedonaContext.create(spark);

        SpatialWriting etl3= new SpatialWriting(spark);

        TableSpec silver = new TableSpec("silverlayer", "nyc_taxi", "");

        TableSpec silverunindexed = new TableSpec("silverlayer2", "nyc_taxi", "");

        String path =   "../datasets/nyc_taxi/yellow_tripdata_2009-04.parquet";

        etl3.customWriter(silver,
                path,150000L, 131072L, "Start_Lon","Start_Lat"
        );

        etl3.customWriterWithoutBboxIndex(silverunindexed,
                path, "Start_Lon","Start_Lat"
        );

        spark.stop();
        Thread.sleep(3000);
    }
}

