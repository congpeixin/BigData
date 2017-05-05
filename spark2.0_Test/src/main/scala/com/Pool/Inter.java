package com.Pool;

/**
 * Created by cluster on 2017/5/2.
 */

class Bird implements Fly {
    int lengh = 2;

    public void fly() {
        System.out.println("我是小鸟");
    }
}

class Ying implements Fly{
    int lengh = 10;

    public void fly() {
        System.out.println("我是老鹰");
    }
}

public class Inter {
    public static void main(String args[]){
        Bird bird = new Bird();
        Ying ying = new Ying();

        bird.fly();
        System.out.println("我的翅膀长：" + bird.lengh);
        ying.fly();
        System.out.println("我的翅膀长：" + ying.lengh);
    }


}
