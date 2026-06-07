package ir.smh.spatialbricks;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ParquetSchemaReader {

    public static void main(String[] args) throws Exception {

        String file =
                "../datasets/newyork/silverlayer/FireStations/data/geometry.bbox_partitioning.region_code=null/00000-51-30b883e7-0c88-471a-9f2a-63362db134f9-0-00001.parquet";

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

        SparkSession spark = SparkSession.builder()
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> df = spark.read().parquet(file);

        df.select("geometry")
                .show(5, false);
    }
}
