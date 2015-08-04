package com.cloudwick.hadoop.binpack;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;

 
public class BinPackingVol {

    public static void main(String[] args) {
        List<Integer> in = Arrays.asList(10, 3, 2, 3, 10, 1, 6, 7, 8);
        
        Map<String, Long> map = new HashMap<String, Long>();
        map.put("a",10L);
        map.put("b",3L);
        map.put("c",2L);
        map.put("d",3L);
        map.put("e",10L);
        map.put("f",1L);
        map.put("g",6L);
        map.put("h",7L);
        map.put("i",8L);
        
        FirstFitDecreasing ffd = new FirstFitDecreasing(map, 12);
        testBinPacking(ffd, "first fit decreasing");
    }

    private static void testBinPacking(FirstFitDecreasing algo, String algoName) {
        long startTime;
        long estimatedTime;

        startTime = System.currentTimeMillis();
        System.out.println("needed bins (" + algoName + "): " + algo.getResult());
        algo.printBestBins();
        estimatedTime = System.currentTimeMillis() - startTime;
        System.out.println("in " + estimatedTime + " ms");

        System.out.println("\n\n");
    }
    
    
    
}
