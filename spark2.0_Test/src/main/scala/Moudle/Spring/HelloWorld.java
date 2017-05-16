package Moudle.Spring;

/**
 * Created by cluster on 2017/5/8.
 */
public class HelloWorld {
    private String message2;
    private String message1;
    public void setMessage1(String message1){
        this.message1  = message1;
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
}