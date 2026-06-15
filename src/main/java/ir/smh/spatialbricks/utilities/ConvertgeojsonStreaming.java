package ir.smh.spatialbricks.utilities;

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.*;

public class ConvertgeojsonStreaming {

    public static void main(String[] args) throws Exception {

        File inputFile = new File("../datasets/internet_and_voice_coverage/voice-bronze-layer/F477_Voice_1412.geojson");
        File outputFile = new File("../datasets/internet_and_voice_coverage/voice-bronze-layer/F477_Voice_1412b.ndjson");

        ObjectMapper mapper = new ObjectMapper();
        JsonFactory factory = mapper.getFactory();

        try (
                JsonParser parser = factory.createParser(inputFile);
                BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile))
        ) {

            while (parser.nextToken() != null) {

                if (parser.currentToken() == JsonToken.FIELD_NAME
                        && "features".equals(parser.getCurrentName())) {

                    parser.nextToken(); // start array

                    while (parser.nextToken() != JsonToken.END_ARRAY) {

                        // حالا بدون خطا کار می‌کند
                        String featureJson = mapper.readTree(parser).toString();

                        writer.write(featureJson);
                        writer.newLine();
                    }

                    break;
                }
            }
        }

        System.out.println("DONE");
    }
}