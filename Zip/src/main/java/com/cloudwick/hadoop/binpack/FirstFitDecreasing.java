package com.cloudwick.hadoop.binpack;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class FirstFitDecreasing{

    private List<Bin> bins = new ArrayList<Bin>();
    private Map<String,Long> in;
    private long binSize;

    public FirstFitDecreasing(Map<String,Long> in, long binSize) {
        this.in = in;
        this.binSize = binSize;
    }

    public List<Bin> getResult() {
    	Map sortedMap = sortByValue(in);
    	Iterator<Map.Entry<String, Long>> entries = sortedMap.entrySet().iterator();
        //Collections.sort(in, Collections.reverseOrder()); // sort input by size (big to small)
        bins.add(new Bin(binSize)); // add first bin
        
        Entry<String,Long> entry = null;
        while(entries.hasNext()){
        	entry = entries.next();
            // iterate over bins and try to put the item into the first one it fits into
            boolean putItem = false; // did we put the item in a bin?
            int currentBin = 0;
            while (!putItem) {
                if (currentBin == bins.size()) {
                    // item did not fit in last bin. put it in a new bin
                    Bin newBin = new Bin(binSize);
                    newBin.put(entry);
                    bins.add(newBin);
                    putItem = true;
                } else if (bins.get(currentBin).put(entry)) {
                    // item fit in bin
                    putItem = true;
                } else {
                    // try next bin
                    currentBin++;
                }
            }
        }
        return bins;
    }
    
    public static Map sortByValue(Map unsortMap) {	 
    	List list = new LinkedList(unsortMap.entrySet());
     
    	Collections.sort(list, new Comparator() {
    		public int compare(Object o1, Object o2) {
    			return ((Comparable) ((Map.Entry) (o1)).getValue())
    						.compareTo(((Map.Entry) (o2)).getValue());
    		}
    	});
     
    	Map sortedMap = new LinkedHashMap();
    	for (Iterator it = list.iterator(); it.hasNext();) {
    		Map.Entry entry = (Map.Entry) it.next();
    		sortedMap.put(entry.getKey(), entry.getValue());
    	}
    	return sortedMap;
    }


    public void printBestBins() {
        System.out.println("Bins:");
        if (bins.size() == in.size()) {
            System.out.println("each item is in its own bin");
        } else {
            for (Bin bin : bins) {
                System.out.println(bin.toString());
            }
        }
    }
}
