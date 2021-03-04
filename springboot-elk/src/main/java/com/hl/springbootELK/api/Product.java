package com.hl.springbootELK.api;

import java.util.HashMap;
import java.util.Map;

public class Product {

    int    id;
    String name;
    String category;
    float  price;
    String place;
    String code;

    public int getId() {
        return id;
    }
    public void setId(int id) {
        this.id = id;
    }
    public String getName() {
        return name;
    }
    public void setName(String name) {
        this.name = name;
    }
    public String getCategory() {
        return category;
    }
    public void setCategory(String category) {
        this.category = category;
    }
    public float getPrice() {
        return price;
    }
    public void setPrice(float price) {
        this.price = price;
    }
    public String getPlace() {
        return place;
    }
    public void setPlace(String place) {
        this.place = place;
    }
 
    public String getCode() {
        return code;
    }
    public void setCode(String code) {
        this.code = code;
    }
    @Override
    public String toString() {
        return "Product [id=" + id + ", name=" + name + ", category=" + category + ", price=" + price + ", place="
                + place + ", code=" + code + "]";
    }

    @SuppressWarnings("rawtypes")
    public Map toMap() {
        HashMap<String,Object> map = new HashMap<>();
        map.put("name", name);
        map.put("category", category);
        map.put("code", code);
        map.put("place", place);
        map.put("price", price);
         
        return map;
    }
}