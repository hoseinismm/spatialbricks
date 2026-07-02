package ir.smh.spatialbricks.create_datasets;

import ir.smh.spatialbricks.encoder.converttogeometry.GeoJsonGeometricalAdapter;
import ir.smh.spatialbricks.udf.WKBIndexedParquet;
import ir.smh.spatialbricks.utilities.PowerPlanUtil;
import ir.smh.spatialbricks.core.SpatialWriting;
import ir.smh.spatialbricks.core.TableSpec;
import ir.smh.spatialbricks.config.SparkConfigLocal;
import ir.smh.spatialbricks.encoder.converttogeometry.GeometryReader;
import ir.smh.spatialbricks.udf.FlattenSpatialParquet;

import org.apache.sedona.spark.SedonaContext;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;

import java.io.IOException;


public class CreatePortoTaxiFromGeoJSON {

    public static void main(String[] args) throws NoSuchTableException, IOException, InterruptedException {
        PowerPlanUtil.setPowerPlan(PowerPlanUtil.SPARK_TEST);

        try {

            String folderpath = "../datasets/portotaxi2";

        var spark = SparkConfigLocal.createSession(folderpath);
            try {

        spark.sparkContext().setLogLevel("ERROR");

        SedonaContext.create(spark);

        GeometryReader<?>  geoparqetFile= new GeoJsonGeometricalAdapter();

        SpatialWriting wkbSpatialWriting= new SpatialWriting(spark,geoparqetFile, new WKBIndexedParquet(spark) );

        SpatialWriting spatialWriting= new SpatialWriting(spark,geoparqetFile );

        SpatialWriting flattenSpatialWriting= new SpatialWriting(spark,geoparqetFile, new FlattenSpatialParquet(spark));

        TableSpec wkbUnindexed = new TableSpec("wkbUnindexed", "portotaxi", folderpath);

        TableSpec wkbIndexed = new TableSpec("wkbIndexed", "portotaxi", folderpath);

        TableSpec silverUnindexed = new TableSpec("silverUnindexed", "portotaxi", folderpath);

        TableSpec silverIndexed = new TableSpec("silverIndexed", "portotaxi", folderpath);

        TableSpec flattenSilverUnindexed = new TableSpec("flattenSilverUnindexed", "portotaxi", folderpath);

        TableSpec flattenSilverIndexed = new TableSpec("flattenSilverIndexed", "portotaxi", folderpath);

        long start = System.currentTimeMillis();

            String path ="../datasets/portotaxi2/portotaxindjson.geojson";

//          wkbSpatialWriting.silverLayerWithoutBboxIndexing(wkbUnindexed, path );

//          wkbSpatialWriting.silverLayerWithBboxIndexing(wkbIndexed,path, 150000L, 131072L);

//          spatialWriting.silverLayerWithoutBboxIndexing(silverUnindexed, path );

//          spatialWriting.silverLayerWithBboxIndexing(silverIndexed,path, 150000L, 131072L);

//          flattenSpatialWriting.silverLayerWithoutBboxIndexing(flattenSilverUnindexed, path );

//          flattenSpatialWriting.silverLayerWithBboxIndexing(flattenSilverIndexed,path, 150000L, 131072L);

        long duration = System.currentTimeMillis() - start;

        System.out.println("Time of writing :  = " + duration);

            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                spark.stop();
            }

        } finally {
            PowerPlanUtil.setPowerPlan(PowerPlanUtil.BALANCED);
        }

        Thread.sleep(3000);
    }
}