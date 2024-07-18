import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class top10extraction {
    public static void main(String[] args) {
        String directory = "C:/Users/גל/Downloads/mevuzrot/output"; // Update this with the actual directory path
        Path outputPath = Paths.get("DecadesTop10.txt"); // The output file

        try (BufferedWriter writer = Files.newBufferedWriter(outputPath)) {
            for (int i = 0; i < 33; i++) {
                String fileName = String.format("part-r-%05d", i);
                Path filePath = Paths.get(directory, fileName);



                try (BufferedReader reader = Files.newBufferedReader(filePath)) {

                    String line = reader.readLine();
                    String [] firstline =line.split(" ",2);
                    writer.write("First 10 collocations of dacade: " + firstline[0]);
                    writer.newLine();
                    writer.newLine();
                    writer.write(firstline[1]);
                    writer.newLine();
                    for (int lineNumber = 1; lineNumber < 10; lineNumber++) {
                         line = reader.readLine();
                        if (line == null) {
                            break; // End of file reached
                        }
                        writer.write(line.split(" ",2)[1]);
                        writer.newLine();
                    }
                } catch (IOException e) {
                    System.err.println("Error reading file: " + fileName);
                }
                writer.newLine();
                writer.newLine();
            }
        } catch (IOException e) {
            System.err.println("Error writing to output file");
        }
    }
}
