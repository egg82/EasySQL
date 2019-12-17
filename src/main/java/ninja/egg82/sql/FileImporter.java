package ninja.egg82.sql;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FileImporter {
    private final Object lineHandlerLock = new Object();
    private StringBuilder lineHandler = new StringBuilder();

    private final SQL sql;
    private String delimiter = ";";

    private static final Pattern DELIMITER_PATTERN = Pattern.compile("^((--)|(\\/\\*[^!])|#|\\/\\/)*\\s*@?DELIMITER\\s+(.*)$", Pattern.CASE_INSENSITIVE);

    public FileImporter(SQL sql) { this.sql = sql; }

    public void readFile(File file, boolean lineByLine) throws SQLException, IOException { readStream(new FileInputStream(file), lineByLine); }

    public CompletableFuture<Void> readFileAsync(File file, boolean lineByLine) {
        return CompletableFuture.runAsync(() -> {
            try {
                readFile(file, lineByLine);
            } catch (SQLException | IOException ex) {
                throw new CompletionException(ex);
            }
        });
    }

    public void readResource(String resourceName, boolean lineByLine) throws SQLException, IOException { readStream(getClass().getClassLoader().getResourceAsStream(resourceName), lineByLine); }

    public CompletableFuture<Void> readResourceAsync(String resourceName, boolean lineByLine) {
        return CompletableFuture.runAsync(() -> {
            try {
                readResource(resourceName, lineByLine);
            } catch (SQLException | IOException ex) {
                throw new CompletionException(ex);
            }
        });
    }

    public void readStream(InputStream stream, boolean lineByLine) throws SQLException, IOException {
        if (lineByLine) {
            synchronized (lineHandlerLock) {
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8.name()))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        readLine(line);
                    }
                } catch (UnsupportedEncodingException ignored) { }
                if (lineHandler.length() > 0) {
                    // Double-check to make sure we didn't catch a few line terminators at the end of the file
                    String last = lineHandler.toString().trim();
                    if (last.length() > 0) {
                        throw new IOException("Missing deliminator at end of file (" + delimiter + ") at '" + lineHandler.toString() + "'.");
                    }
                }
            }
        } else {
            StringBuilder builder = new StringBuilder();
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8.name()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    builder.append(line);
                    builder.append('\n');
                }
            } catch (UnsupportedEncodingException ignored) { }
            readString(builder.toString());
        }
    }

    public CompletableFuture<Void> readStreamAsync(InputStream stream, boolean lineByLine) {
        return CompletableFuture.runAsync(() -> {
            try {
                readStream(stream, lineByLine);
            } catch (SQLException | IOException ex) {
                throw new CompletionException(ex);
            }
        });
    }

    public void readString(String contents) throws SQLException { sql.execute(contents); }

    public CompletableFuture<Void> readStringAsync(String contents) {
        return CompletableFuture.runAsync(() -> {
            try {
                readString(contents);
            } catch (SQLException ex) {
                throw new CompletionException(ex);
            }
        });
    }

    private void readLine(String line) throws SQLException {
        line = line.trim();
        if (line.length() == 0) {
            return;
        }

        if (line.charAt(0) == '#' || (line.startsWith("/*") && !line.startsWith("/*!")) || line.startsWith("--") || line.startsWith("//")) {
            return;
        } else {
            Matcher matcher = DELIMITER_PATTERN.matcher(line);
            if (matcher.matches()) {
                delimiter = matcher.group(4);
            } else if (line.contains(delimiter)) {
                lineHandler.append(line, 0, line.lastIndexOf(delimiter));
                lineHandler.append('\n');
                sql.execute(lineHandler.toString());
                lineHandler.setLength(0);
                if (line.lastIndexOf(delimiter) < line.length() - 1) {
                    lineHandler.append(line.substring(line.lastIndexOf(delimiter)));
                    lineHandler.append('\n');
                }
            } else {
                lineHandler.append(line);
                lineHandler.append('\n');
            }
        }
    }
}
