package ir.smh.spatialbricks.utilities;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;



public class ParquetStats {

    public static void main(String[] args) throws Exception {

        String file =
                "../datasets/newyork/silverlayer/FireStations/data/geometry.bbox_partitioning.region_code=null/00000-51-09fb7f0e-c9fa-4997-9952-076301d4c384-0-00001.parquet";

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
