package ir.smh.spatialbricks.encoder;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
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
                .add("min_x", DataTypes.DoubleType, false)
                .add("min_y", DataTypes.DoubleType, false)
                .add("max_x", DataTypes.DoubleType, false)
                .add("max_y", DataTypes.DoubleType, false)
                .add("region_code", DataTypes.LongType, false);

        return df
                .withColumn(
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
                                        .alias("bbox_partitioning")


                        )
                )
                .select(geometryColumnName);

    }
}
