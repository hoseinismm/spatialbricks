package ir.smh.spatialbricks;

import ir.smh.spatialbricks.core.AddOrUpdateBboxIndex;
import ir.smh.spatialbricks.core.SpatialWriting;
import ir.smh.spatialbricks.core.TableSpec;
import ir.smh.spatialbricks.encoder.converttogeometry.GeoJsonGeometricalAdapter;
import ir.smh.spatialbricks.encoder.converttogeometry.GeometryReader;
import ir.smh.spatialbricks.encoder.converttogeometry.WKBReaderAdapter;
import ir.smh.spatialbricks.encoder.converttogeometry.WKTReaderAdapter;
import ir.smh.spatialbricks.udf.FlattenSpatialParquet;
import ir.smh.spatialbricks.udf.SpatialParquet;
import ir.smh.spatialbricks.udf.UDFRegistry;
import ir.smh.spatialbricks.udf.WKBIndexedParquet;
import org.apache.sedona.spark.SedonaContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;

import java.io.IOException;
import java.util.Objects;

public class SpatialBricks {

    private final SparkSession spark;
    private final AddOrUpdateBboxIndex index;
    private final SpatialWriting writer;

    public enum InputFormat {
        GEOJSON,
        WKB,
        WKT
    }

    public enum GeometryFormat {
        WKB,
        SPATIAL_PARQUET,
        FLATTEN_SPATIAL_PARQUET
    }

    private static class ReaderFactory {

        static GeometryReader<?> create(InputFormat format) {
            return switch (format) {
                case GEOJSON -> new GeoJsonGeometricalAdapter();
                case WKB -> new WKBReaderAdapter();
                case WKT -> new WKTReaderAdapter();
            };
        }
    }

    private static class RegistryFactory {

        static UDFRegistry<?, ?> create(
                GeometryFormat format,
                SparkSession spark) {

            return switch (format) {
                case WKB -> new WKBIndexedParquet(spark);
                case SPATIAL_PARQUET -> new SpatialParquet(spark);
                case FLATTEN_SPATIAL_PARQUET -> new FlattenSpatialParquet(spark);
            };
        }
    }

    public SpatialBricks(
            SparkSession spark,
            InputFormat inputFormat,
            GeometryFormat geometryFormat) {

        this.spark = Objects.requireNonNull(spark);

        GeometryReader<?> geometryReader =
                ReaderFactory.create(inputFormat);

        UDFRegistry<?, ?> udfRegistry =
                RegistryFactory.create(geometryFormat, spark);

        udfRegistry.registerDecode();
        udfRegistry.registerGeometryUdf(geometryReader);

        this.index = new AddOrUpdateBboxIndex(spark, udfRegistry);
        this.writer = new SpatialWriting(spark, udfRegistry);

        SedonaContext.create(spark);
    }

    private TableSpec table(
            String database,
            String table) {

        String catalog = spark.catalog().currentCatalog();

        String warehouse =
                spark.conf().get(
                        "spark.sql.catalog."
                                + catalog
                                + ".warehouse");

        return new TableSpec(database, table, warehouse);
    }

    /*----------------------------------------------------
     * Writing
     *---------------------------------------------------*/

    public void write(
            String database,
            String table,
            Dataset<Row> df)
            throws NoSuchTableException {

        writer.silverLayerWithoutBboxIndexing(
                table(database, table),
                df);
    }

    public void write(
            String database,
            String table,
            String inputPath)
            throws Exception {

        writer.silverLayerWithoutBboxIndexing(
                table(database, table),
                inputPath);
    }

    public void writeWithIndex(
            String database,
            String table,
            Dataset<Row> df,
            long driverRows,
            long maxPartitionSize)
            throws NoSuchTableException {

        writer.silverLayerWithBboxIndexing(
                table(database, table),
                df,
                driverRows,
                maxPartitionSize);
    }

    public void writeWithIndex(
            String database,
            String table,
            String inputPath,
            long driverRows,
            long maxPartitionSize)
            throws Exception {

        writer.silverLayerWithBboxIndexing(
                table(database, table),
                inputPath,
                driverRows,
                maxPartitionSize);
    }

    /*----------------------------------------------------
     * Indexing
     *---------------------------------------------------*/

    public void addIndexToNewRows(
            String database,
            String table,
            long driverRows,
            long maxPartitionSize)
            throws NoSuchTableException, IOException {

        index.addIndexToUnindexedRows(
                table(database, table),
                driverRows,
                maxPartitionSize);
    }

    public void rebuildIndex(
            String database,
            String table,
            long driverRows,
            long maxPartitionSize)
            throws NoSuchTableException, IOException {

        index.updateIndexing(
                table(database, table),
                driverRows,
                maxPartitionSize);
    }
}