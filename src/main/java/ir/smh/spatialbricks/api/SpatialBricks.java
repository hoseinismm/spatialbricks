package ir.smh.spatialbricks.api;

import ir.smh.spatialbricks.config.SparkConfig;
import ir.smh.spatialbricks.core.AddOrUpdateIndex;
import ir.smh.spatialbricks.core.PipelineExecutor;
import ir.smh.spatialbricks.core.TableSpec;
import ir.smh.spatialbricks.encoder.converttogeometry.GeoJsonGeometricalAdapter;
import ir.smh.spatialbricks.encoder.converttogeometry.GeometryReader;
import ir.smh.spatialbricks.encoder.converttogeometry.WKBReaderAdapter;
import ir.smh.spatialbricks.encoder.converttogeometry.WKTReaderAdapter;
import ir.smh.spatialbricks.udf.*;
import org.apache.sedona.spark.SedonaContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.parser.ParseException;

import java.io.IOException;
import java.util.Objects;


public class SpatialBricks {

    private final SparkSession spark;
    private final AddOrUpdateIndex index;
    private final PipelineExecutor writer;



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
                case SP -> new SpatialParquet(spark);
                case FSP -> new FlattenSpatialParquet(spark);
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

        this.index = new AddOrUpdateIndex(spark, udfRegistry);
        this.writer = new PipelineExecutor(spark, udfRegistry);

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
            throws NoSuchTableException, ParseException {

        writer.AddDataWithoutIndexing(
                table(database, table),
                df);
    }

    public void write(
            String database,
            String table,
            String inputPath)
            throws Exception {

        writer.AddDataWithoutIndexing(
                table(database, table),
                inputPath);
    }

    public void write(
            String database,
            String table,
            String inputPath,
            String columnx,
            String columny

    ) throws Exception {
        writer.xyToPointTableWithoutIndexing(
                table(database, table),
                inputPath,
                columnx, columny
        );
    }

    public void writeWithIndex(
            String database,
            String table,
            Dataset<Row> df,
            long driverRows,
            long maxPartitionSize)
            throws NoSuchTableException, ParseException {

        writer.AddDataWithIndexing(
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

        writer.AddDataWithIndexing(
                table(database, table),
                inputPath,
                driverRows,
                maxPartitionSize);
    }

    public void writeWithIndex(
            String database,
            String table,
            String inputPath,
            String columnx,
            String columny,
            long driverRows,
            long maxPartitionSize

    ) throws Exception {
        writer.xyToPintTableWithIndexing(
                table(database, table),
                inputPath,
                driverRows,
                maxPartitionSize,
                columnx, columny
        );
    }


    /*----------------------------------------------------
     * Indexing
     *---------------------------------------------------*/

    public void addIndexToNewRows(
            String database,
            String table,
            long driverRows,
            long maxPartitionSize)
            throws NoSuchTableException, IOException, ParseException {

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
            throws NoSuchTableException, IOException, ParseException {

        index.updateIndexing(
                table(database, table),
                driverRows,
                maxPartitionSize);
    }
}