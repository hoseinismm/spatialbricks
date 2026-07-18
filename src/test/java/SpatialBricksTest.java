import ir.smh.spatialbricks.api.SpatialBricks;
import ir.smh.spatialbricks.config.SparkConfigLocal;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static ir.smh.spatialbricks.api.SpatialBricks.InputFormat.*;
import static ir.smh.spatialbricks.api.SpatialBricks.GeometryFormat.*;

import static org.ejml.UtilEjml.assertTrue;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class SpatialBricksTest {

    static SparkSession spark;
    static String folderpath;

    @BeforeAll
    static void setUp() {
        folderpath ="../datasets/nyc_taxi";
        try {

            SparkSession spark = SparkConfigLocal.createSession(folderpath);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @AfterAll
    static void tearDown() {
        spark.stop();
    }

    @Test
    void createGeometryColumnFromOtherColumns() {
        SpatialBricks sb = new SpatialBricks(
                spark,
                WKB,
                WKB_PARQUET
        );

//        sb.writeWithIndex();

        assertTrue(spark.catalog().tableExists("db.my_table"));
    }
}
