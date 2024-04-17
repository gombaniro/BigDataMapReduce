package org.bigdata.common;

import java.util.ArrayList;
import java.util.List;

public class WindowMaker {

    public static List<ProductPair> make(List<String> purchases ) {
        List<ProductPair> pairs = new ArrayList<>();

        for (int i = 0; i < purchases.size(); i++) {
            String key = purchases.get(i);
            List<String> products = new ArrayList<>();
            for (int j = i + 1; j < purchases.size() ; j++) {
                if (purchases.get(j).equals(key)) {
                    break;
                }
                products.add(purchases.get(j));
            }
            pairs.add(new ProductPair(key, products));
        }
        return pairs;
    }

    public static void main(String[] args) {
        System.out.println(WindowMaker.make(List.of("a", "b", "c", "a", "d", "e")));
    }
}
