package ir.smh.spatialbricks.utilities;


import ir.smh.spatialbricks.config.SparkConfigLocal;
import ir.smh.spatialbricks.udf.SpatialParquet;
import ir.smh.spatialbricks.udf.UDFRegistry;
import ir.smh.spatialbricks.udf.WKBIndexedParquet;
import org.apache.iceberg.Table;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.actions.SparkActions;
import org.apache.sedona.spark.SedonaContext;
import org.apache.sedona.sql.utils.SedonaSQLRegistrator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.catalyst.parser.ParseException;

import java.io.IOException;

public class QueryMain {
    public static void main(String[] args) throws IOException, NoSuchTableException, ParseException, TableAlreadyExistsException {

        var spark = SparkConfigLocal.createSession("../datasets/nyc_taxi");
        spark.sparkContext().setLogLevel("ERROR");
        SedonaContext.create(spark);
        UDFRegistry udfRegistry=new SpatialParquet(spark);
        udfRegistry.registerDecode();
        SedonaSQLRegistrator.registerAll(spark);
//        spark.sql("""
//                SELECT
//              ST_AsText(ST_GeomFromWKB(geometry.geom))
//          FROM wkbIndexed.aubuildings
//          LIMIT 20;
//        """).show(false);

        Table table = Spark3Util.loadIcebergTable(
                spark,
                "SilverIndexed.nyc_taxi");
        SparkActions
                .get(spark)
                .rewriteDataFiles(table)
                .execute();
        SparkActions.get(spark)
                .expireSnapshots(table)
                .expireOlderThan(System.currentTimeMillis())
                .execute();
        SparkActions.get(spark)
                .deleteOrphanFiles(table)
                .execute();



//
//        TableSpec silver = new TableSpec("silverlayer", "FireStations", "");
//
//        BucketServiceForBboxIndexing a= new BucketServiceForBboxIndexing(spark);
//        a.updateBucket(silver);
//


//
//
//
//        spark.sql("""
//                    SELECT sum(ST_NumGeometries(decodeGeometry(geometry)))
//                    FROM silverIndexed.portotaxi
//
//                """).show();
/*
        spark.sql("""
                    SELECT
                        file_path,
                        partition,
                        lower_bounds['geometry.center.x'] AS min_x,
                        upper_bounds['geometry.center.x'] AS max_x,
                        lower_bounds['geometry.center.y'] AS min_y,
                        upper_bounds['geometry.center.y'] AS max_y
                    FROM silverlayer.FireStations.files
                """).show(false);


        // پیام اطلاع‌رسانی برای مانیتور Spark UI
        System.out.println("✅ Spark SQL job finished successfully.");
        System.out.println("🚀 Spark UI is available at: http://localhost:4040");
        System.out.println("⏳ Press ENTER to stop the program...");

         */
//
//        spark.sql("""
//    SELECT
//        geometry.bbox_partitioning.min_x AS min_x,
//        geometry.bbox_partitioning.max_x AS max_x,
//        geometry.bbox_partitioning.min_y AS min_y,
//        geometry.bbox_partitioning.max_y AS max_y,
//        geometry.bbox_partitioning.region_code AS code,
//
//        COUNT(*) AS cnt
//    FROM silverlayer.FireStations
//    GROUP BY
//        geometry.bbox_partitioning.min_x,
//        geometry.bbox_partitioning.max_x,
//        geometry.bbox_partitioning.min_y,
//        geometry.bbox_partitioning.max_y,
//        geometry.bbox_partitioning.region_code
//
//    ORDER BY cnt DESC
//""").show(false);


         /*


        spark.sql("""
        SELECT count(*)
        FROM spark_catalog.silverlayer.FireStations
        """).show(false);








        spark.sql("""
                SELECT *
                FROM spark_catalog.silverlayer.FireStations
                WHERE exists(
                  geometry.part,
                  p -> exists(p.coordinate, c -> c.x > -74)
                )
        """).show(false);

        spark.sql("""          
                SELECT count(*) AS silver
                FROM  silverlayer.FireStations
                WHERE geometry.PART[0].COORDINATE[0].x > -74.0
         """).show();
        var table = Spark3Util.loadIcebergTable(
                spark,
                "silverlayer.FireStations"
        );
        System.out.println(table.schema());


/*spark_catalog.silverlayer.FireStations





var table = Spark3Util.loadIcebergTable(
        spark,
        "silverlayer.FireStations"
);

var snapshot = table.currentSnapshot();

for (var manifest : snapshot.allManifests(table.io())) {

    System.out.println("Manifest: " + manifest.path());

    try (var reader =
                 ManifestFiles.read(manifest, table.io())) {

        for (var file : reader) {

            System.out.println(file.path());

            System.out.println("Lower Bounds:");
            System.out.println(file.lowerBounds());

            System.out.println("Upper Bounds:");
            System.out.println(file.upperBounds());
        }
    }
}

// جلوگیری از بسته شدن برنامه (برای مشاهده Spark UI)



//----------------------------







var schema = table.schema();



for (var manifest : snapshot.allManifests(table.io())) {

    System.out.println("Manifest: " + manifest.path());

    try (var reader =
                 ManifestFiles.read(manifest, table.io())) {

        for (var file : reader) {

            System.out.println("FILE: " + file.path());

            Map<Integer, ByteBuffer> lowerBounds =
                    file.lowerBounds();

            Map<Integer, ByteBuffer> upperBounds =
                    file.upperBounds();

            for (Map.Entry<Integer, ByteBuffer> entry :
                    lowerBounds.entrySet()) {

                Integer fieldId = entry.getKey();

                var field = schema.findField(fieldId);

                if (field == null) {
                    continue;
                }

                Object lower =
                        Conversions.fromByteBuffer(
                                field.type(),
                                entry.getValue()
                        );

                Object upper =
                        Conversions.fromByteBuffer(
                                field.type(),
                                upperBounds.get(fieldId)
                        );

                System.out.println(
                        "fieldId=" + fieldId +
                                ", name=" + field.name() +
                                ", lower=" + lower +
                                ", upper=" + upper
                );
            }
        }
    }
}

//         */
//System.in.read();
//
//
spark.stop();



}
}
