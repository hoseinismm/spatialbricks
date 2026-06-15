package ir.smh.spatialbricks.create_datasets;

import ir.smh.spatialbricks.SpatialWriting;
import ir.smh.spatialbricks.TableSpec;
import ir.smh.spatialbricks.config.SparkConfig;
import ir.smh.spatialbricks.encoder.GeometryReader;
import ir.smh.spatialbricks.encoder.udf.converttogeometry.WKBReaderAdapter;
import ir.smh.spatialbricks.encoder.udf.converttogeometry.geoJsonGeometricalAdapter;
import org.apache.sedona.spark.SedonaContext;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;

import java.io.IOException;


public class CreatePortoTaxi {

    public static void main(String[] args) throws NoSuchTableException, IOException, InterruptedException {

        var spark = SparkConfig.createSession("../datasets/portotaxi");

        spark.sparkContext().setLogLevel("ERROR");

        SedonaContext.create(spark);

        GeometryReader<?>  geoparqetFile= new WKBReaderAdapter();

        SpatialWriting spatialWriting= new SpatialWriting(spark,geoparqetFile );

        TableSpec silver = new TableSpec("silverlayer", "portotaxi", "");

        TableSpec silverunindexed = new TableSpec("silverlayer2", "portotaxi", "");

        for (int i=0; i<35; i++) {
            String path = String.format("../datasets/portotaxi/porto_taxi_chunk_%d.parquet", i);
            spatialWriting.silverLayerWithBboxIndexing(silver,
                    path, 150000L, 131072L
            );
            spatialWriting.silverLayerWithoutBboxIndexing(silverunindexed, path );
        }

        spark.stop();
        Thread.sleep(3000);
    }
}

