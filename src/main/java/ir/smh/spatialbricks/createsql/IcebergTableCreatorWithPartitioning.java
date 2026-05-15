package ir.smh.spatialbricks.createsql;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;
import java.io.Serializable;
import java.util.List;

public class IcebergTableCreatorWithPartitioning implements Serializable {

    /**
     * ساخت جدول Iceberg از روی Schema با امکان پارتیشن بندی
     *
     * @param spark        SparkSession
     * @param schema       StructType جدول
     * @param databaseName نام دیتابیس
     * @param tableName    نام جدول
     * @param partitionExpressions لیست عبارات پارتیشن، مثلاً ["bucket(50, center_x_int)", "identity(city)"]
     */
    public static void createIcebergTableFromSchema(
            SparkSession spark,
            StructType schema,
            String databaseName,
            String tableName,
            List<String> partitionExpressions
    ) {
        String fullTableName = String.format("%s.%s", databaseName, tableName);
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE IF NOT EXISTS ").append(fullTableName).append(" (\n");

        for (StructField field : schema.fields()) {
            String name = field.name();
            String sqlType;
            DataType dt = field.dataType();

            // نگاشت نوع داده‌ها به SQL
            if (dt instanceof StringType) {
                sqlType = "STRING";
            } else if (dt instanceof IntegerType) {
                sqlType = "INT";
            } else if (dt instanceof LongType) {
                sqlType = "BIGINT";
            } else if (dt instanceof DoubleType) {
                sqlType = "DOUBLE";
            } else if (dt instanceof FloatType) {
                sqlType = "FLOAT";
            } else if (dt instanceof BooleanType) {
                sqlType = "BOOLEAN";
            } else if (dt instanceof TimestampType) {
                sqlType = "TIMESTAMP";
            } else if (dt instanceof DateType) {
                sqlType = "DATE";
            } else {
                // برای ساختارهای پیچیده (Array, Struct, Map)
                sqlType = dt.catalogString();
            }

            sb.append("    `").append(name).append("` ").append(sqlType).append(",\n");
        }

        // حذف کامای آخر
        sb.setLength(sb.length() - 2);
        sb.append("\n) USING iceberg");

        // اگر پارتیشن تعریف شده باشد
        if (partitionExpressions != null && !partitionExpressions.isEmpty()) {
            sb.append("\nPARTITIONED BY (");
            for (String expr : partitionExpressions) {
                sb.append(expr).append(", ");
            }
            sb.setLength(sb.length() - 2); // حذف کامای آخر
            sb.append(")");
        }

        String createSql = sb.toString();

        // اطمینان از وجود دیتابیس
        spark.sql("CREATE DATABASE IF NOT EXISTS " + databaseName);

        // ساخت جدول
        spark.sql(createSql);

        System.out.println("✅ جدول ساخته شد:\n" + createSql);
    }

}
