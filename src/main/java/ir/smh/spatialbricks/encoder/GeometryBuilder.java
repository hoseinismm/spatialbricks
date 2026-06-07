package ir.smh.spatialbricks.encoder;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import static org.apache.spark.sql.functions.*;

import java.io.Serializable;

public final class GeometryBuilder implements Serializable {

    private GeometryBuilder() {
    }

    public static Dataset<Row> addPointGeometryColumn(
            Dataset<Row> df,
            String xColumn,
            String yColumn,
            String geometryColumnName
    ) {

        StructType bboxType = new StructType()
                .add("min_x", "double", false)
                .add("min_y", "double", false)
                .add("max_x", "double", false)
                .add("max_y", "double", false)
                .add("region_code", "integer", false);

        StructType bucketRangeType = new StructType()
                .add("floor", "integer", false)
                .add("ceiling", "integer", false);

        return df.withColumn(
                geometryColumnName,
                struct(
                        lit(1).alias("type"),

                        array(
                                struct(
                                        array(
                                                struct(
                                                        col(xColumn).alias("x"),
                                                        col(yColumn).alias("y")
                                                )
                                        ).alias("coordinates")
                                )
                        ).alias("parts"),

                        lit(null)
                                .cast(bboxType)
                                .alias("bbox_partitioning"),

                        lit(null)
                                .cast(bucketRangeType)
                                .alias("geohash_partitioning")
                )
        );

    }
}
