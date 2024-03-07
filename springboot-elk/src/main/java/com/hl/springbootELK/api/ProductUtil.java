package com.hl.springbootELK.api;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.springframework.util.ResourceUtils;

public class ProductUtil {

    public static void main(String[] args) throws IOException {
        String fileName = "140k_products.txt";
        File file = ResourceUtils.getFile("classpath:" + fileName);
        List<Product> file2List = file2List(file);
        System.out.println(file2List.size());
    }

    static List<Product> file2List(File file) throws IOException {
        List<String>  lines    = FileUtils.readLines(file);
        List<Product> products = new ArrayList<>();
        for(String line : lines) {
            Product line2product = line2product(line);
            products.add(line2product);
        }

        return products;
    }

    private static Product line2product(String line) {
        Product  product = new Product();
        String[] lines   = line.split(",");
        product.setId(Integer.parseInt(lines[0]));
        product.setName(lines[1]);
        product.setCategory(lines[2]);
        product.setPrice(Float.parseFloat(lines[3]));
        product.setPlace(lines[4]);
        product.setCode(lines[5]);

        return product;
    }
}