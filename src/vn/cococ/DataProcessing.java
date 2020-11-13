package vn.cococ;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;


public class DataProcessing {
    public static void findMostPopularCategory(String sourceFile) {
        try {
            // Read file
            FileReader fr = new FileReader(sourceFile);
            BufferedReader br = new BufferedReader(fr);

            String line;
            HashMap<String, Integer> occ = new HashMap<String, Integer>();
            String mostPopularCategory = "";
            int maxOccurrence = 0;

            while ((line = br.readLine()) != null) {
                if (line.equals("")){
                    continue;
                }
                DataStream data = new DataStream(line);

                for (int i = 0; i < data.category.length; i++) {
                    // Check if category_id already exists on HashMap or not
                    if (occ.containsKey(data.category[i])) {
                        occ.replace(data.category[i], occ.get(data.category[i]) + data.occurrence[i]);
                    } else {
                        occ.put(data.category[i], data.occurrence[i]);
                    }

                    // Set the most popular category
                    if (occ.get(data.category[i]) > maxOccurrence) {
                        mostPopularCategory = data.category[i];
                        maxOccurrence = occ.get(data.category[i]);
                    }
                }
            }
            System.out.println("1. Most popular category is " + mostPopularCategory + " with " + maxOccurrence + " counts");
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
    public static void findLargestAppearedCategory(String sourceFile){
        try {
            // Read file
            FileReader fr = new FileReader(sourceFile);
            BufferedReader br = new BufferedReader(fr);

            String line;
            HashMap<String, Integer> occ = new HashMap<String, Integer>();
            String largestAppearedCategory = "";
            int maxAppearedTime = 0;

            while ((line = br.readLine()) != null){

                if (line.equals("")){
                    continue;
                }
                DataStream data = new DataStream(line);

                for (int i=0; i<data.category.length; i++){
                    // Check if category_id already exists on HashMap or not
                    if (occ.containsKey(data.category[i])) {
                        occ.replace(data.category[i], occ.get(data.category[i]) + 1);
                    }else {
                        occ.put(data.category[i], data.occurrence[i]);
                    }

                    // Set the most popular category
                    if (occ.get(data.category[i]) > maxAppearedTime){
                        largestAppearedCategory = data.category[i];
                        maxAppearedTime = occ.get(data.category[i]);
                    }
                }
            }
            System.out.println("2. Largest appeared category is " + largestAppearedCategory + " with " + maxAppearedTime + " times");
        } catch(Exception ex){
            ex.printStackTrace();
        }
    }
    public static void main(String[] args) {
        String dataSource = "data/hash_catid_count.csv";
        findMostPopularCategory(dataSource);
        findLargestAppearedCategory(dataSource);
    }
}
