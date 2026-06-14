package ir.smh.spatialbricks.create_datasets;

import ir.smh.spatialbricks.SpatialWriting;
import ir.smh.spatialbricks.TableSpec;
import ir.smh.spatialbricks.config.SparkConfig;
import org.apache.sedona.spark.SedonaContext;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;

import java.io.IOException;



public class CreateUkrainFlight {

    SpatialWriting spatialWriting;

    public static void main(String[] args) throws NoSuchTableException, IOException, InterruptedException {

        var spark = SparkConfig.createSession("../datasets/ukrainflights");

        spark.sparkContext().setLogLevel("ERROR");

        SedonaContext.create(spark);

        SpatialWriting etl3= new SpatialWriting(spark);

        TableSpec silver = new TableSpec("silverlayer", "ukrainflights", "");

        TableSpec silverunindexed = new TableSpec("silverlayer2", "ukrainflights", "");

        String path =   "../datasets/ukrainflights/ukraine_coords.parquet";

        etl3.customWriter(silver,
                path,150000L, 131072L, "lon","lat"
        );

        etl3.customWriterWithoutBboxIndex(silverunindexed,
                path, "lon","lat"
        );

        spark.stop();
        Thread.sleep(3000);
    }
}

