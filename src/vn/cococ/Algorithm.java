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
    public static int getMinIndex(List<DataStreamInSplitFile> list){
        DataStreamInSplitFile minItem = list.get(0);
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

    public static int compare(DataStreamInSplitFile a, DataStreamInSplitFile b, String sortedBy){
        if (sortedBy.equals("String")){
            return a.getOrderIdAsString().compareTo(b.getOrderIdAsString());
        }
        return a.getOrderIdAsInteger().compareTo(b.getOrderIdAsInteger());
    }


    public static void addNewData(List<DataStreamInSplitFile> ordered_list,
                                  DataStreamInSplitFile newData,
                                  String sortedBy){
        int first = 0;
        int last = ordered_list.size() - 1;
        System.out.println(ordered_list.size());
        if (ordered_list.size() == 0)
            ordered_list.add(newData);
        else{
            while (last - first > 1){
                int mid = (last+first) / 2;

                if (compare(newData, ordered_list.get(mid), sortedBy) <= 0){
                    last = mid;
                }else{
                    first = mid;
                }
            }

            // Insert new
            if (compare(newData, ordered_list.get(first), sortedBy) <= 0){
                ordered_list.add(first, newData);
            }else{
                ordered_list.add(first + 1, newData);
            }
        }

    }

    /*
        Split large file into multiple smaller file. Each file not exceed maxLine lines
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
            List<String> splitFilePaths = paths.filter(Files::isRegularFile)
                    .map(Path::toString).collect(Collectors.toList());

            List<BufferedReader> brs = new ArrayList<>();
            List<DataStreamInSplitFile> cache = new ArrayList<>();
            for (int i=0; i<splitFilePaths.size(); i++){
                BufferedReader br = new BufferedReader(new FileReader(splitFilePaths.get(i)));
                String line = br.readLine();
                if (line.length() > 0){
                    cache.add(new DataStreamInSplitFile(line, i));
                }
                brs.add(br);
            }

            if (sortedBy.equals("String"))
                cache.sort(Comparator.comparing(DataStreamInSplitFile::getOrderIdAsString));
            if (sortedBy.equals("int"))
                cache.sort(Comparator.comparing(DataStreamInSplitFile::getOrderIdAsInteger));

            for (DataStreamInSplitFile data: cache){
                System.out.print(data.objectId + "\t");
            }
            System.out.println();
            BufferedWriter bw = new BufferedWriter(new FileWriter(sortedFile));
            while (true){
                // Get and remove min Data
                if (cache.size() > 0){
                    DataStreamInSplitFile minData = cache.remove(0);

                    bw.write(minData.toString());

                    bw.newLine();
                    String line = brs.get(minData.file).readLine();
                    if (line != null && !line.equals("")){
                        addNewData(cache, new DataStreamInSplitFile(line, minData.file), sortedBy);
                    }
//                    System.out.println(cache.size());
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
        String destinationFolder = "split_data_string";
        String sortedFile = "data/hash_catid_count_string_sorted.csv";

        String sortedBy = "String";

        // Split file
//        splitFile(sourceFile, destinationFolder, maxLine, sortedBy);

        // Combine file
        combineFile(destinationFolder, sortedFile, sortedBy);

//        System.out.println(Integer.compare(9, 10));

//        int x = 6;
//        int[] arr = {1, 3, 4, 5, 6, 8, 9, 11, 12};
//        int first = 0;
//        int last = arr.length - 1;
//        int mid = 0;
//        while (last - first != 1){
//            mid = (last+first)/2;
//
//            if (x > arr[mid])
//                first = mid;
//            else
//                last = mid;
//            System.out.println(first + " - " +  last);
//
//        }
//        System.out.println(first + " - " +  last);
//
    }
}
