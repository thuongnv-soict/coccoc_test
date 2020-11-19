package vn.cococ;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class Test {
    public static void main(String[] args){
//        System.out.println(Integer.compare(15, 10));
        List<BigInteger> x = new ArrayList<BigInteger>();
        x.add(new BigInteger("5"));
        x.add(new BigInteger("1"));
        x.add(new BigInteger("2"));

        x.sort(BigInteger::compareTo);

        System.out.println(x);
//        System.out.println("12345".compareTo("1334"));
    }
}
