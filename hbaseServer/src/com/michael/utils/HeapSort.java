package com.michael.utils;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by hadoop on 17-4-15.
 */
public class HeapSort {
    //方法作用:取出list里面的最小的 k 个值
    public static <T extends Comparable<T>> List<T> sort(List<T> list, int k) throws Exception {
        if (k <= 0) {
            throw new Exception("k 必须大于0");
        }
        if (list.size() < k) {
            throw new Exception("list 长度必须大于k");
        }
        List<T> heapList = new ArrayList<T>(k);
        for (int i = 0; i < k; i ++) {
            heapList.add(list.get(i));
        }
        initialHeap(heapList);
        for (int i = k; i < list.size(); i ++) {
            if (list.get(i).compareTo(heapList.get(0)) < 0) {
                heapList.set(0, list.get(i));
                heapify(heapList, k, 0);
            }
        }
        return heapList;
    }
    public static <T extends Comparable<T>> void initialHeap(List<T> list) {
        int n = list.size();
        // Build heap (rearrange array)
        for (int i = n / 2 - 1; i >= 0; i--)
            heapify(list, n, i);
    }
    public static <T extends Comparable<T>> void heapify(List<T> list, int n, int i)
    {
        int largest = i;  // Initialize largest as root
        int l = 2*i + 1;  // left = 2*i + 1
        int r = 2*i + 2;  // right = 2*i + 2

        // If left child is larger than root
        if (l < n && (list.get(l).compareTo(list.get(largest)) > 0))
            largest = l;

        // If right child is larger than largest so far
        if (r < n && (list.get(r).compareTo(list.get(largest)) > 0))
            largest = r;

        // If largest is not root
        if (largest != i)
        {
            T swap = list.get(i);
            list.set(i, list.get(largest));
            list.set(largest, swap);
            // Recursively heapify the affected sub-tree
            heapify(list, n, largest);
        }
    }
}
