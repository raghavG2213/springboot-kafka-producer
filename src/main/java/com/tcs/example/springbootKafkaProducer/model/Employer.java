package com.tcs.example.springbootKafkaProducer.model;

public class Employer {

    private String name;

    public Employer(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
