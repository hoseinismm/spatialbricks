package ir.smh.spatialbricks.utilities;


import ir.smh.spatialbricks.TableSpec;
import ir.smh.spatialbricks.config.SparkConfig;
import ir.smh.spatialbricks.encoder.GeometryResult;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

public class ParquetSchemaReader {

    public static void main(String[] args) throws Exception {

        String file =
                "../datasets/newyork/raw-files/yellow_tripdata_2009-01.parquet";

        ParquetMetadata metadata =
                ParquetFileReader.readFooter(
                        new Configuration(),
                        new Path(file)
                );

        MessageType schema =
                metadata.getFileMetaData().getSchema();

        System.out.println("PARQUET SCHEMA");
        System.out.println("==============");
        System.out.println(schema);

        var spark = SparkConfig.createSession("../datasets/newyork");
        /*

        Dataset<Row> df = spark.read().parquet(file);
        TableSpec bronze = new TableSpec("bronzelayer", "FireStations", "");

        df=df.select("Start_Lon","Start_Lat");

        IcebergTableCreator.createIcebergTableFromSchema(
                spark,
                df.schema(),
                bronze.database(),
                bronze.table()
        );

        String fullName = bronze.database() + "." + bronze.table();
        System.out.println(df.count());

        spark.table("silverlayer.FireStations")
                .select("geometry")
                .show(5, false);

        df.writeTo(fullName).append();
        */


        TableSpec silver = new TableSpec("silverlayer", "FireStations", "");
        String fullName = silver.database() + "." + silver.table();

        Dataset<Row> table = spark.read()
                .format("iceberg")
                .load(fullName);

        spark.udf().register(
                "geoNum",
                (Double x, Double y) -> GeometryResult.computeGeohashNumeric(x, y),
                DataTypes.IntegerType
        );

        table = table.orderBy(functions.expr("geoNum(Start_Lon, Start_Lat)"));

        table.drop("geoNum");
        table.writeTo("silverlayer.FireStations")
                .overwritePartitions();

         spark.sql("""
                            CALL spark_catalog.system.expire_snapshots(
                                table => 'silverlayer.FireStations',
                                older_than => TIMESTAMP '2100-01-01 00:00:00',
                                retain_last => 1
                            )
            """);

                    spark.sql("""
            SELECT *
            FROM silverlayer.FireStations.snapshots
            """).show(false);
            
    }

}
