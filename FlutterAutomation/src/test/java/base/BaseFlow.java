package base;

import io.appium.java_client.android.AndroidDriver;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class BaseFlow {

    protected AndroidDriver driver;

    private static BufferedWriter logWriter;

    private static final DateTimeFormatter FILE_DATE_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss");

    private static final DateTimeFormatter LOG_DATE_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public BaseFlow(AndroidDriver driver) {

        this.driver = driver;

        initializeLogFile();
    }

    /**
     * Initializes automation log file.
     *
     * Creates:
     * logs/automation_YYYY-MM-DD_HH-mm-ss.txt
     */
    private synchronized void initializeLogFile() {

        if (logWriter != null) {
            return;
        }

        try {

            File logDirectory = new File("logs");

            if (!logDirectory.exists()) {

                logDirectory.mkdirs();

                System.out.println(
                        "===== LOG DIRECTORY CREATED ====="
                );
            }

            String timestamp = LocalDateTime.now()
                    .format(FILE_DATE_FORMAT);

            String logFileName =
                    "automation_" + timestamp + ".txt";

            File logFile = new File(
                    logDirectory,
                    logFileName
            );

            logWriter = new BufferedWriter(
                    new FileWriter(logFile, true)
            );

            System.out.println(
                    "===== LOG FILE CREATED ====="
            );

            System.out.println(
                    logFile.getAbsolutePath()
            );

            writeToFile(
                    "AUTOMATION LOG STARTED"
            );

        } catch (IOException e) {

            System.err.println(
                    "Unable to create automation log file"
            );

            e.printStackTrace();
        }
    }

    /**
     * Prints formatted log message
     * and writes it to log file.
     */
    protected void log(String message) {

        String timestamp = LocalDateTime.now()
                .format(LOG_DATE_FORMAT);

        String formattedMessage =
                "\n"
                + "==================================================\n"
                + "[" + timestamp + "] "
                + message
                + "\n"
                + "==================================================\n";

        System.out.println(formattedMessage);

        writeToFile(formattedMessage);
    }

    /**
     * Prints current page source
     * and writes it to log file.
     */
    protected void printPageSource() {

        String pageSource;

        try {

            pageSource = driver.getPageSource();

        } catch (Exception e) {

            log(
                    "FAILED TO GET PAGE SOURCE: "
                    + e.getMessage()
            );

            return;
        }

        String formattedPageSource =
                "\n"
                + "=============== PAGE SOURCE START ===============\n"
                + pageSource
                + "\n"
                + "================ PAGE SOURCE END ================\n";

        System.out.println(formattedPageSource);

        writeToFile(formattedPageSource);
    }

    /**
     * Writes content to automation log file.
     */
    private static synchronized void writeToFile(
            String content
    ) {

        if (logWriter == null) {
            return;
        }

        try {

            logWriter.write(content);

            logWriter.newLine();

            logWriter.flush();

        } catch (IOException e) {

            System.err.println(
                    "Unable to write automation log"
            );

            e.printStackTrace();
        }
    }

    /**
     * Closes automation log file.
     */
    public static synchronized void closeLogFile() {

        if (logWriter == null) {
            return;
        }

        try {

            writeToFile(
                    "AUTOMATION LOG COMPLETED"
            );

            logWriter.close();

            logWriter = null;

            System.out.println(
                    "===== AUTOMATION LOG FILE CLOSED ====="
            );

        } catch (IOException e) {

            System.err.println(
                    "Unable to close automation log file"
            );

            e.printStackTrace();
        }
    }
}