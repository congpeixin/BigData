package Moudle.Spring;

/**
 * Created by cluster on 2017/5/9.
 */
public class HelloWorld_1 {
    private String message1;
    private String message2;
    private String message3;
    public void setMessage1(String message){
        this.message1  = message;
    }
    public void getMessage1(){
        System.out.println("Your Message : " + message1);
    }

    public void getMessage2() {
        System.out.println("Your Message : " + message2);
    }

    public void setMessage2(String message2) {
        this.message2 = message2;
    }

    public void getMessage3() {
        System.out.println("Your Message : " + message3);
    }

    public void setMessage3(String message) {
        this.message3 = message;
    }
}
