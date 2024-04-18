package org.bigdata.common;

import java.util.List;

public class ProductPair {
    private final String key;
    private final List<String> products;

    public ProductPair(String key, List<String> products) {
        this.key = key;
        this.products = products;
    }

    public String getKey() {
        return key;
    }

    public List<String> getProducts() {
        return products;
    }

    @Override
    public String toString() {
        return "ProductPair{" +
                "key='" + key + '\'' +
                ", products=" + products +
                '}';
    }
}
