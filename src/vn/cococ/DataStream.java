package vn.cococ;

import java.math.BigInteger;
import java.util.Arrays;

public class DataStream {
    public String objectId;
    public String[] category;
    public int[] occurrence;

    public DataStream(String line) {
        String[] partition = line.split("\\s");
        this.objectId = partition[0];
        this.category = partition[1].substring(1, partition[1].length()-1).split(",");
        String[] occurrence = partition[2].substring(1, partition[2].length()-1).split(",");
        this.occurrence = new int[this.category.length];
        for (int i=0; i<occurrence.length; i++){
            this.occurrence[i] =  Integer.parseInt(occurrence[i]);
        }
    }

    public String getOrderIdAsString(){
        return this.objectId;
    }

    public BigInteger getOrderIdAsInteger(){
        return new BigInteger(this.objectId);
    }

    public String toString(){
        String outputString = this.objectId;
        outputString += "\t" + Arrays.toString(this.category).replace(" ", "");
        outputString += "\t" + Arrays.toString(this.occurrence).replace(" ", "");
        return outputString;
    }
}