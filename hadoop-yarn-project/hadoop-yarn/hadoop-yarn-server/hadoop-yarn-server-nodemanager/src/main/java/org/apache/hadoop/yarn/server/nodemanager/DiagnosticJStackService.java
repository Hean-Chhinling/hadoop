package org.apache.hadoop.yarn.server.nodemanager;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.util.Shell;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DiagnosticJStackService {

    private static final Logger LOG = LoggerFactory
            .getLogger(DiagnosticJStackService.class);
    private static final String PYTHON_COMMAND = "python3";


    private static String scriptLocation = null;

    static {
        try {
            // Extract script from JAR to a temp file
            InputStream in = DiagnosticJStackService.class.getClassLoader()
                    .getResourceAsStream("diagnostics/jstack_collector.py");
            File tempScript = File.createTempFile("jstack_collector", ".py");
            Files.copy(in, tempScript.toPath(), StandardCopyOption.REPLACE_EXISTING);
            tempScript.setExecutable(true); // Set execute permission
            scriptLocation = tempScript.getAbsolutePath();
        } catch (IOException e) {
            LOG.error("Failed to extract Python script from JAR", e);
        }
    }



    public static List<String> collectJStack(String appId)
            throws Exception {
        if (Shell.WINDOWS) {
            throw new UnsupportedOperationException("Not implemented for Windows.");
        }
        ProcessBuilder pb = createProcessBuilder(appId);

        LOG.info("Diagnostic process environment: {}", pb.environment());

        return executeCommand(pb);
    }


    @VisibleForTesting
    protected static ProcessBuilder createProcessBuilder(String appId) {
        List<String> commandList =
                new ArrayList<>(Arrays.asList(PYTHON_COMMAND, scriptLocation, appId));

        return new ProcessBuilder(commandList);
    }

    private static List<String> executeCommand(ProcessBuilder pb)
            throws Exception {
        Process process = pb.start();
        int exitCode;
        List<String> result = new ArrayList<>();

        try (
                BufferedReader stdoutReader = new BufferedReader(new InputStreamReader(process.getInputStream(),
                        StandardCharsets.UTF_8));
                BufferedReader stderrReader = new BufferedReader(new InputStreamReader(process.getErrorStream(),
                        StandardCharsets.UTF_8));
        ) {

            String line;
            while ((line = stdoutReader.readLine()) != null) {
                result.add(line);
            }

            List<String> errors = new ArrayList<>();
            while ((line = stderrReader.readLine()) != null) {
                errors.add(line);
            }
            if (!errors.isEmpty()) {
                LOG.error("Python script stderr: {}", errors);
            }

            process.waitFor();
        } catch (Exception e) {
            LOG.error("Error getting JStack: {}", pb.command());
            throw e;
        }
        exitCode = process.exitValue();
        if (exitCode != 0) {
            throw new IOException("The JStack collector script exited with non-zero " +
                    "exit code: " + exitCode);
        }

        return result;
    }

}
