package com.cloudwick.hadoop.binpack;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;

/**
 * A Bin holding integers.
 * <br/>
 * The number of items it can hold is not limited, but the added value of the
 * items it holds may not be higher than the given maximal size.
 */
public class Bin {

    /**
     * maximal allowed added value of items.
     */
    protected long maxSize;
    /**
     * current added value of items.
     */
    protected long currentSize;
    /**
     * list of items in bin.
     */
    protected Map<String,Long> items;

    /**
     * construct new bin with given maximal size.
     *
     * @param maxSize
     */
    public Bin(long maxSize) {
        this.maxSize = maxSize;
        this.currentSize = 0;
        this.items = new HashMap<String,Long>();
    }

    /**
     * adds given item to this bin, and increases the currentSize of the bin by
     * value of item. If item does not fit, it will not be put in the bin and
     * false will be returned.
     *
     * @param item item to put in bin
     * @return true if item fit in bin, false otherwise
     */
    public boolean put(Entry<String, Long> item) {
        if (currentSize + item.getValue() <= maxSize) {
            items.put(item.getKey(), item.getValue());
            currentSize += item.getValue();
            return true;
        } else {
            return false; // item didn't fit
        }
    }

    /**
     * removes given item from bin and reduces the currentSize of the bin by
     * value of item.
     *
     * @param item item to remove from bin
     */
    public void remove(Entry<String, Long> item) {
        items.remove(item.getKey());
        currentSize -= item.getValue();
    }

    /**
     * returns the number of items in this bin (NOT the added value of the
     * items).
     *
     * @return number of items in this bin
     */
    public int numberOfItems() {
        return items.size();
    }

    /**
     * creates a deep copy of this bin.
     *
     * @return deep copy of this bin
     */
    public Bin deepCopy() {
        Bin copy = new Bin(0);
        copy.items = new HashMap<String,Long>(items); // Integers are not copied by reference
        copy.currentSize = currentSize;
        copy.maxSize = maxSize;
        return copy;
    }

    @Override
    public String toString() {
        String res = "";
//        Iterator<Map.Entry<String, Long>> entries = items.entrySet().iterator(); 
//        while(entries.hasNext()) { 
//        	
//        	//res = StringUtils.join(items.keySet(), ",");
//            res += entries.next().getKey() + " ";
//        }
        res +=StringUtils.join(items.keySet(), ",");
        //res += "    Size: " + currentSize + " (max: " + maxSize + ")";
        return res;
    }
}
