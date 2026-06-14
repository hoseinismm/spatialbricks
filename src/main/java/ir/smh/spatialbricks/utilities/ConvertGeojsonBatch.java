package ir.smh.spatialbricks.utilities;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

import java.io.*;

public class ConvertGeojsonBatch {
    public static void main(String[] args) throws IOException {
        // مسیر پوشه ورودی و خروجی
        String inputDir = "../datasets/newyork/raw-files/";
        String outputDir = "../datasets/newyork/raw-files/";

        // نام فایل‌های ورودی
        String[] inputFiles = {
                "group_id_0.geojson",
                "group_id_1.geojson",
                "group_id_2.geojson",
                "group_id_3.geojson",
                "group_id_4.geojson"
        };

        ObjectMapper mapper = new ObjectMapper();

        for (String fileName : inputFiles) {
            File inputFile = new File(inputDir + fileName);
            String outputName = fileName.replace(".geojson", "_ndjson.json");
            File outputFile = new File(outputDir + outputName);

            System.out.println("⏳ در حال تبدیل: " + fileName);

            JsonNode root = mapper.readTree(inputFile);
            ArrayNode features = (ArrayNode) root.get("features");

            try (BufferedWriter writer = new BufferedWriter(
                    new OutputStreamWriter(new FileOutputStream(outputFile), "UTF-8"))) {
                for (JsonNode feature : features) {
                    writer.write(mapper.writeValueAsString(feature));
                    writer.write("\n");
                }
            }

            System.out.println("✅ تبدیل انجام شد: " + outputFile.getAbsolutePath());
        }

        System.out.println("🎉 همه‌ی فایل‌ها با موفقیت تبدیل شدند.");
    }
}
