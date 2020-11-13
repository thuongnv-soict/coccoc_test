package vn.cococ;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Algorithm {
    public static void generateFile(List<DataStream> list, String filename){
        try {
            FileWriter myWriter = new FileWriter(filename);
            for (DataStream item: list){
                myWriter.write(item.toString());
                myWriter.write("\n");
            }

            myWriter.close();
            System.out.println("Successfully write "+ filename);
        } catch (IOException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }
    }

    /*
        Get index of DataStream has smallest orderId in a list
     */
    public static int getMinIndex(List<DataStream> list){
        DataStream minItem = list.get(0);
        int minIndex = 0;

        for (int i=1; i<list.size(); i++){
            if (list.get(i) != null) {
                if (minItem == null){
                    minItem = list.get(i);
                    minIndex = i;
                }
                else if (list.get(i).objectId.compareTo(minItem.objectId) < 0) {
                    minItem = list.get(i);
                    minIndex = i;
                }
            }
        }

        if (minItem == null){
            return -1;
        }

        return minIndex;
    }

    /*
        Split large file into multiple smaller file. Each file not exceed maxLine lines
     */
    public static void splitFile(String sourceFile, String destinationFolder, int maxLine){
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
                    splitData.sort(Comparator.comparing(DataStream::getOrderId));
                    count = 0;
                    generateFile(splitData, destinationFolder + "/data_"+fileIndex+".txt");
                    splitData.clear();
                    fileIndex++;
                }
            }
            if (count > 0){
                generateFile(splitData, destinationFolder + "/data_"+fileIndex+".txt");
            }

        } catch(Exception ex){
            ex.printStackTrace();
        }
    }

    /*
        Combine all sorted files in a folder to a file
     */
    public static void combineFile(String destinationFolder, String sortedFile){

        // Walk through all files in destinationFolder
        try (Stream<Path> paths = Files.walk(Paths.get(destinationFolder))) {
            List<String> result = paths.filter(Files::isRegularFile)
                    .map(Path::toString).collect(Collectors.toList());

            List<BufferedReader> brs = new ArrayList<>();
            List<DataStream> cache = new ArrayList<>();
            for (String filePath: result){
                BufferedReader br = new BufferedReader(new FileReader(filePath));
                String line = br.readLine();
                if (line.length() > 0){
                    cache.add(new DataStream(line));
                }
                brs.add(br);
            }

            BufferedWriter bw = new BufferedWriter(new FileWriter(sortedFile));
            while (true){
                int minIndex = getMinIndex(cache);
                if (minIndex != -1) {
                    bw.write(cache.get(minIndex).toString());
                    bw.newLine();
                    String line = brs.get(minIndex).readLine();
                    if (line == null || line.equals("")){
                        cache.set(minIndex, null);
                    }
                    else{
                        cache.set(minIndex, new DataStream(line));
                    }
                } else {
                    break;
                }
            }
            System.out.println("Successfully create file having sorted object_id: "+ sortedFile);
            // Close files
            for (BufferedReader br: brs){
                br.close();
            }
            bw.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static void main(String[] args) {
        int maxLine = 20000;
        String sourceFile = "data/hash_catid_count.csv";
        String destinationFolder = "split_data";
        String sortedFile = "data/hash_catid_count_sorted.csv";

        // Split file
        splitFile(sourceFile, destinationFolder, maxLine);

        // Combine file
        combineFile(destinationFolder, sortedFile);

    }
}
