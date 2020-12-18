package com.github.aly8246.catalog;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class A {
    public static class Student {
        public Integer id;
        public String name;
        public String classRoom;

        public Student(String classRoom) {
            this.id = new Random().nextInt();
            this.name = UUID.randomUUID().toString();
            this.classRoom=classRoom;
        }

        @Override
        public String toString() {
            return this.id+"-"+this.name+"-"+this.classRoom;
        }
    }

    public static void main(String[] args) {
        List<Student> aClass = Arrays.asList(
                new Student("a班")
                ,new Student("a班")
                ,new Student("a班")
                ,new Student("a班"));

        List<Student> bClass = Arrays.asList(
                new Student("b班")
                ,new Student("b班")
                ,new Student("b班")
                ,new Student("b班"));

        List<List<Student>> resultList = Stream.of(aClass, bClass)
                .map(e -> e.stream()
                        .filter(student -> student.name.startsWith("1"))
                        .collect(Collectors.toList()))
                .collect(Collectors.toList());
        System.out.println(resultList);

    }
}
