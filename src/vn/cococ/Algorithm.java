package vn.cococ;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class DataStreamInSplitFile extends DataStream{
    public int file;

    public DataStreamInSplitFile(String line, int file) {
        super(line);
        this.file = file;
    }
}
public class Algorithm {
    public static void generateFile(List<DataStream> list, String outputFilePath){
        try {
            FileWriter myWriter = new FileWriter(outputFilePath);
            for (DataStream item: list){
                myWriter.write(item.toString());
                myWriter.write("\n");
            }

            myWriter.close();
            System.out.println("Successfully write "+ outputFilePath);
        } catch (IOException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }
    }

    /*
        Compare two Data Stream objects by their objectId.
        Parameter 'sortedBy' stand for comparing by 'String' or 'int'
        a > b: return 1
        a = b: return 0
        a < b: return -1
     */
    public static int compare(DataStreamInSplitFile a, DataStreamInSplitFile b, String sortedBy){
        if (sortedBy.equals("String")){
            return a.getOrderIdAsString().compareTo(b.getOrderIdAsString());
        }
        return a.getOrderIdAsInteger().compareTo(b.getOrderIdAsInteger());
    }

    /*
        Add new object to ordered list,
        Time complexity: O(log(n))
     */
    public static void addNewData(List<DataStreamInSplitFile> orderedList,
                                  DataStreamInSplitFile newData,
                                  String sortedBy){
        // In case orderedList is empty
        if (orderedList.size() == 0)
            orderedList.add(newData);
        else{
            int first = 0;
            int last = orderedList.size() - 1;
            while (last - first > 1){
                int mid = (last+first) / 2;

                if (compare(newData, orderedList.get(mid), sortedBy) <= 0){
                    last = mid;
                } else {
                    first = mid;
                }
            }

            // Insert new object
            if (compare(newData, orderedList.get(first), sortedBy) <= 0){
                orderedList.add(first, newData);
            } else if (compare(newData, orderedList.get(last), sortedBy) >= 0){
                orderedList.add(last + 1, newData);
            } else {
                orderedList.add(first + 1, newData);
            }
        }

    }

    /*
        Split large file into multiple smaller file. Each file will not exceed 'maxLine' lines
     */
    public static void splitFile(String sourceFile, String destinationFolder, int maxLine, String sortedBy){
        try {
            // Read source file
            FileReader fr = new FileReader(sourceFile);
            BufferedReader br = new BufferedReader(fr);

            String line;
            int count = 0;
            int fileIndex = 0;
            List<DataStream> splitData = new ArrayList<>();

            // Read each line
            while ((line = br.readLine()) != null){
                if (line.equals("")){
                    continue;
                }
                DataStream data = new DataStream(line);
                splitData.add(data);
                count++;

                // Generate file
                if (count == maxLine) {
                    if (sortedBy.equals("String"))
                        splitData.sort(Comparator.comparing(DataStream::getOrderIdAsString));
                    if (sortedBy.equals("int"))
                        splitData.sort(Comparator.comparing(DataStream::getOrderIdAsInteger));

                    count = 0;
                    generateFile(splitData, destinationFolder + "/data_" + sortedBy+ "_" + fileIndex + ".txt");
                    splitData.clear();
                    fileIndex++;
                }
            }
            if (count > 0){
                generateFile(splitData, destinationFolder + "/data_" + sortedBy+ "_" + fileIndex + ".txt");
            }

        } catch(Exception ex){
            ex.printStackTrace();
        }
    }

    /*
        Combine all sorted files in a folder to a file
     */
    public static void combineFile(String destinationFolder, String sortedFile, String sortedBy){

        // Walk through all files in destinationFolder
        try (Stream<Path> paths = Files.walk(Paths.get(destinationFolder))) {
            // Get list of path to files in a folder
            List<String> splitFilePaths = paths.filter(Files::isRegularFile)
                    .map(Path::toString).collect(Collectors.toList());

            List<BufferedReader> brs = new ArrayList<>();
            BufferedWriter bw = new BufferedWriter(new FileWriter(sortedFile));
            List<DataStreamInSplitFile> cache = new ArrayList<>();

            // Initialize buffer readers and 'cache' - a list contains all first record in each file
            for (int i=0; i<splitFilePaths.size(); i++){
                BufferedReader br = new BufferedReader(new FileReader(splitFilePaths.get(i)));
                String line = br.readLine();
                if (line.length() > 0){
                    cache.add(new DataStreamInSplitFile(line, i));
                }
                brs.add(br);
            }

            // Sort 'cache' to easy when getting smallest object and inserting new object
            if (sortedBy.equals("String"))
                cache.sort(Comparator.comparing(DataStreamInSplitFile::getOrderIdAsString));
            if (sortedBy.equals("int"))
                cache.sort(Comparator.comparing(DataStreamInSplitFile::getOrderIdAsInteger));

            while (true){
                if (cache.size() > 0){
                    // Get and remove object has smallest orderId in 'cache'
                    DataStreamInSplitFile smallestData = cache.remove(0);

                    // Write smallestData in output file
                    bw.write(smallestData.toString());
                    bw.newLine();

                    // Insert new object from the file which have the above removed object to 'cache'
                    String line = brs.get(smallestData.file).readLine();
                    if (line != null && !line.equals("")){
                        addNewData(cache, new DataStreamInSplitFile(line, smallestData.file), sortedBy);
                    }
                } else {
                    break;
                }
            }
            System.out.println("Successfully create file having sorted objectId: "+ sortedFile);

            // Close files
            for (BufferedReader br: brs){
                br.close();
            }
            bw.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void sortFileByObjectId(String sourceFile, String outputFolder, int maxLine, String sortedBy){
        // Validate 'sortedBy'
        if (!sortedBy.equals("String") && !sortedBy.equals("int")){
            System.out.println("sortedBy must be \"String\" or \"int\"");
            System.exit(1);
        }

        String destinationFolderForSplitFile = "split_data_" + sortedBy;

        // Creating the destinationFolder
        File splitFile = new File(destinationFolderForSplitFile);
        if (!splitFile.exists()){
            boolean bool = splitFile.mkdir();
            if(bool){
                System.out.println("Directory " + destinationFolderForSplitFile + " created successfully");
            }else{
                System.out.println("Sorry couldn't create specified directory");
            }
        }

        // Creating the output folder
        File outputFile = new File(outputFolder);
        if (!outputFile.exists()){
            boolean bool = outputFile.mkdir();
            if(bool){
                System.out.println("Directory " + outputFile + " created successfully");
            }else{
                System.out.println("Sorry couldn't create specified directory");
            }
        }

        String outputFileName = "output/sortedBy_"+sortedBy+".txt";
        splitFile(sourceFile, destinationFolderForSplitFile, maxLine, sortedBy);
        combineFile(destinationFolderForSplitFile, outputFileName, sortedBy);

    }

    public static void main(String[] args) {
        int maxLine = 20000;
        String sourceFile = "data/hash_catid_count.csv";
        String outputFolder = "output";

        // Test when comparing objectId by 'String'
        sortFileByObjectId(sourceFile, outputFolder, maxLine, "String");

        // Test when comparing objectId by 'int'
        sortFileByObjectId(sourceFile, outputFolder, maxLine, "int");

    }
}
