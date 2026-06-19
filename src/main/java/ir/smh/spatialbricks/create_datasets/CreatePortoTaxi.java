package ir.smh.spatialbricks.create_datasets;

import ir.smh.spatialbricks.SpatialWriting;
import ir.smh.spatialbricks.TableSpec;
import ir.smh.spatialbricks.config.SparkConfig;
import ir.smh.spatialbricks.config.SparkConfigLocal;
import ir.smh.spatialbricks.encoder.GeometryReader;
import ir.smh.spatialbricks.encoder.udf.FlattenSpatialParquet;
import ir.smh.spatialbricks.encoder.udf.UDFRegistry;
import ir.smh.spatialbricks.encoder.udf.converttogeometry.WKBReaderAdapter;
import ir.smh.spatialbricks.encoder.udf.converttogeometry.geoJsonGeometricalAdapter;
import org.apache.sedona.spark.SedonaContext;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;

import java.io.IOException;


public class CreatePortoTaxi {

    public static void main(String[] args) throws NoSuchTableException, IOException, InterruptedException {

        var spark = SparkConfigLocal.createSession("../datasets/portotaxi");

        spark.sparkContext().setLogLevel("ERROR");

        SedonaContext.create(spark);

        GeometryReader<?>  geoparqetFile= new WKBReaderAdapter();

        UDFRegistry udfRegistry=new FlattenSpatialParquet();

        SpatialWriting spatialWriting= new SpatialWriting(spark,geoparqetFile );

        SpatialWriting flattenSpatialWriting= new SpatialWriting(spark,geoparqetFile, udfRegistry );

        TableSpec silverIndexed = new TableSpec("silverIndexed", "portotaxi", "");

        TableSpec silverUnindexed = new TableSpec("silverUnindexed", "portotaxi", "");

        TableSpec flattenSilverUnindexed = new TableSpec("flattenSilverUnindexed", "portotaxi", "");

        TableSpec flattenSilverIndexed = new TableSpec("flattenSilverIndexed", "portotaxi", "");

        long start = System.currentTimeMillis();

        for (int i=0; i<35; i++) {
            String path = String.format("../datasets/portotaxi/porto_taxi_chunk_%d.parquet", i);
//          spatialWriting.silverLayerWithBboxIndexing(silverIndexed,path, 150000L, 131072L);
            spatialWriting.silverLayerWithoutBboxIndexing(silverUnindexed, path );
//          flattenSpatialWriting.silverLayerWithoutBboxIndexing(flattenSilverUnindexed, path );//
//          flattenSpatialWriting.silverLayerWithBboxIndexing(flattenSilverIndexed,path, 150000L, 131072L);
        }

        System.out.println("Process time" +
                " for writing = " + (System.currentTimeMillis() - start));

        System.out.println("Press ENTER to exit...");
        System.in.read();

        spark.stop();
        Thread.sleep(3000);
    }
}