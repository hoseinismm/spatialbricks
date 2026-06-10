package ir.smh.spatialbricks;


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
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.size;
import static org.apache.spark.sql.functions.sum;

public class fortestsqls {

    public static void main(String[] args) throws Exception {

        //String file =  "../datasets/portotaxi/portotaxi.geojson";

        var spark = SparkConfig.createSession("../datasets/ukrainflights");

        TableSpec silver = new TableSpec("silverlayer", "ukrainflights", "");

        String fullName = silver.database() + "." + silver.table();

        Dataset<Row> table = spark.read()
                .format("iceberg")
                .load(fullName);

        table.show(5,false);

        //System.out.println(totalPoints);
    }
}
