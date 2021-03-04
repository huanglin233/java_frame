package com.hl.cloud.entities;

import java.io.Serializable;

public class Payment implements Serializable{

    private static final long serialVersionUID = 1L;

    private Long   id;
    private String serial;

    public Payment() {};

    public Payment(Long id, String serial) {
        this.id     = id;
        this.serial = serial;
    }

    public Long getId() {
        return id;
    }
    public void setId(Long id) {
        this.id = id;
    }
    public String getSerial() {
        return serial;
    }
    public void setSerial(String serial) {
        this.serial = serial;
    }
}