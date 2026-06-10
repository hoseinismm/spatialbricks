package ir.smh.spatialbricks;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

import java.io.*;

public class Convertgeojson {
    public static void main(String[] args) throws IOException {
        // مسیر فایل ورودی (GeoJSON با FeatureCollection)
        File inputFile = new File("../datasets/portotaxi/portotaxi.geojson");
        // مسیر فایل خروجی (NDJSON)
        File outputFile = new File("../datasets/portotaxi/portotaxiconverted.geojson");

        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = mapper.readTree(inputFile);

        // آرایه‌ی features
        ArrayNode features = (ArrayNode) root.get("features");

        try (BufferedWriter writer = new BufferedWriter(
                new OutputStreamWriter(new FileOutputStream(outputFile), "UTF-8"))) {
            for (JsonNode feature : features) {
                // هر Feature به صورت خط جداگانه نوشته می‌شود
                writer.write(mapper.writeValueAsString(feature));
                writer.write("\n");
            }
        }

        System.out.println("✅ تبدیل انجام شد: " + outputFile.getAbsolutePath());
    }
}
