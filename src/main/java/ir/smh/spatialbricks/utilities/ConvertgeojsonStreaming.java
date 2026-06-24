package ir.smh.spatialbricks.utilities;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;

public class ConvertgeojsonStreaming {

    public static void main(String[] args) throws Exception {

        File inputFile = new File("../datasets/portotaxi/portotaxi.geojson");
        File outputFile = new File("../datasets/portotaxi2/portotaxindjson.geojson");

        ObjectMapper mapper = new ObjectMapper();
        JsonFactory factory = mapper.getFactory();

        try (
                JsonParser parser = factory.createParser(inputFile);
                BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile))
        ) {

            while (parser.nextToken() != null) {

                if (parser.currentToken() == JsonToken.FIELD_NAME
                        && "features".equals(parser.getCurrentName())) {

                    parser.nextToken(); // START_ARRAY

                    while (parser.nextToken() != JsonToken.END_ARRAY) {

                        JsonNode feature = mapper.readTree(parser);

                        if (feature instanceof ObjectNode objectNode) {

                            // اگر properties وجود نداشت یا null بود، یک شیء خالی اضافه کن
                            if (!objectNode.has("properties")
                                    || objectNode.get("properties").isNull()) {
                                objectNode.putObject("properties");
                            }

                            writer.write(mapper.writeValueAsString(objectNode));
                            writer.newLine();
                        }
                    }

                    break;
                }
            }
        }

        System.out.println("DONE");
    }
}