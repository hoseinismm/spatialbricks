package ir.smh.spatialbricks;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;



public class ParquetStats {

    public static void main(String[] args) throws Exception {

        String file =
                "../datasets/newyork/silverlayer/FireStations/data/bucket_min=424673280/00000-10-d13c47d9-740a-4f52-8f42-3374185cbc86-0-00001.parquet";

        var reader = ParquetFileReader.open(
                new Configuration(),
                new Path(file)
        );

        var footer = reader.getFooter();

        for (BlockMetaData block : footer.getBlocks()) {

            System.out.println("ROW GROUP");
            System.out.println("----------------");

            for (ColumnChunkMetaData column : block.getColumns()) {

                String path = column.getPath().toDotString();

                var stats = column.getStatistics();

                System.out.println("Column: " + path);

                if (stats != null && !stats.isEmpty()) {

                    System.out.println("Min: " + stats.genericGetMin());
                    System.out.println("Max: " + stats.genericGetMax());
                    System.out.println("Nulls: " + stats.getNumNulls());

                } else {
                    System.out.println("No stats");
                }

                System.out.println();
            }
        }

        reader.close();
    }
}
