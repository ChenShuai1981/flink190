package com.caselchen.flink;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class Person {
    private String name;
    private int age;
    private String result;

    public Person(String name, int age) {
        this.name = name;
        this.age = age;
    }
}
