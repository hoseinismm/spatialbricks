package ir.smh.spatialbricks;


import ir.smh.spatialbricks.config.SparkConfig;
import org.apache.sedona.spark.SedonaContext;
import org.apache.sedona.sql.utils.SedonaSQLRegistrator;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.parser.ParseException;

import java.io.IOException;





public class QueryMain {
    public static void main(String[] args) throws IOException, NoSuchTableException, ParseException {




        var spark = SparkConfig.createSession("../datasets/newyork");
        spark.sparkContext().setLogLevel("ERROR");

        SedonaContext.create(spark);
        SedonaSQLRegistrator.registerAll(spark);
        /*

        spark.sql("""
                    SELECT COUNT(*) AS inside_count
                    FROM silverlayer.FireStations
                    WHERE geometry.center.x BETWEEN -74.0419 AND -73.8334
                      AND geometry.center.y BETWEEN 40.5702 AND 40.7394
                """).show();

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

        spark.sql("""          
                SELECT count(*) AS bronzecount
                FROM spark_catalog.bronzelayer.FireStations
                WHERE ST_X(ST_GeomFromGeoJSON(geometry)) > -74.0
         """).show();


         /*


        spark.sql("""
        SELECT count(*)
        FROM spark_catalog.silverlayer.FireStations
        """).show(false);

          */






        spark.sql("""
SELECT *
FROM spark_catalog.silverlayer.FireStations
WHERE exists(
  geometry.part,
  p -> exists(p.coordinate, c -> c.x > -74)
)


""").show(false);

        spark.sql("""          
                SELECT count(*) AS silvercount
                FROM spark_catalog.silverlayer.FireStations
                WHERE geometry.PART[0].COORDINATE[0].x > -74.0
         """).show();


/*





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

         */
System.in.read();


spark.stop();



}
}
