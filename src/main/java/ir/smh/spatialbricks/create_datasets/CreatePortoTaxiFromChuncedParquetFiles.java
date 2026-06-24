package ir.smh.spatialbricks.create_datasets;

import ir.smh.spatialbricks.utilities.PowerPlanUtil;
import ir.smh.spatialbricks.core.SpatialWriting;
import ir.smh.spatialbricks.core.TableSpec;
import ir.smh.spatialbricks.config.SparkConfigLocal;
import ir.smh.spatialbricks.encoder.converttogeometry.GeometryReader;
import ir.smh.spatialbricks.encoder.udf.FlattenSpatialParquet;
import ir.smh.spatialbricks.encoder.udf.UDFRegistry;
import ir.smh.spatialbricks.encoder.converttogeometry.WKBReaderAdapter;
import org.apache.sedona.spark.SedonaContext;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;

import java.io.IOException;


public class CreatePortoTaxiFromChuncedParquetFiles {

    public static void main(String[] args) throws NoSuchTableException, IOException, InterruptedException {
        PowerPlanUtil.setPowerPlan(PowerPlanUtil.SPARK_TEST);

        try {

        var spark = SparkConfigLocal.createSession("../datasets/portotaxi");
            try {

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
        long duration = System.currentTimeMillis() - start;

        System.out.println("Time of writing :  = " +duration);

            } finally {
                spark.stop();
            }

        } finally {
            PowerPlanUtil.setPowerPlan(PowerPlanUtil.BALANCED);
        }

        Thread.sleep(3000);
    }
}