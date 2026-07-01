package ir.smh.spatialbricks.udf;

import ir.smh.spatialbricks.core.BucketManagerForBboxIndexing;
import ir.smh.spatialbricks.core.TableSpec;
import ir.smh.spatialbricks.encoder.converttogeometry.GeometryReader;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;

public interface UDFRegistry<T,U> {
    DataType getGeometryType();
    void registerGeometryUdf(GeometryReader<?> adapter);
    void registerBucketUdf(Broadcast<BucketManagerForBboxIndexing.Bucket> broadcast);
    void registerBboxUdf();
    void registerDecode();
    Dataset<Row> addPointGeometryColumn(
            Dataset<Row> df,
            String xColumn,
            String yColumn,
            String geometryColumnName
    );
    U parse(Geometry geometry);
    Row geometryToRow(T geometry);
    Geometry geometryToJTS(Row geoRow) throws ParseException;

}
