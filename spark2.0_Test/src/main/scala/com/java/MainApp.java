package com.java;

/**
 * Created by cluster on 2017/5/8.
 */
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
public class MainApp {

    public static void main(String[] args) {
        ApplicationContext context =
                new ClassPathXmlApplicationContext("applicationContext.xml");
        HelloWorld obj = (HelloWorld) context.getBean("helloWorld");
        obj.getMessage1();
        obj.getMessage2();

        HelloWorld_1 obj1 = (HelloWorld_1) context.getBean("helloWorld1");
        obj1.getMessage1();
        obj1.getMessage2();
        obj1.getMessage3();

    }
}
