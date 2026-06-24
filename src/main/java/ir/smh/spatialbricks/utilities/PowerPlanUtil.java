package ir.smh.spatialbricks.utilities;

import java.io.IOException;

public class PowerPlanUtil {

    public static final String BALANCED =
            "381b4222-f694-41f0-9685-ff5bb260df2e";

    public static final String SPARK_TEST =
            "f7e3bc43-51fa-4a80-bd78-3a7453d2cfe6";

    /**
     * فعال کردن Power Plan بر اساس GUID
     *
     * @param guid شناسه پاور پلن
     * @throws IOException
     * @throws InterruptedException
     */
    public static void setPowerPlan(String guid) throws IOException, InterruptedException {

        Process process = new ProcessBuilder(
                "powercfg",
                "/setactive",
                guid
        ).inheritIO().start();

        int exitCode = process.waitFor();

        if (exitCode != 0) {
            throw new RuntimeException("Failed to change power plan. Exit code: " + exitCode);
        }
    }
}