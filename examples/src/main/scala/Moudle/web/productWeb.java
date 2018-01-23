package Moudle.web;

import java.io.*;

/**
 * Created by root on 2017/11/28.
 */
public class productWeb {

    //  文件路径
    private static String filepath = "C:\\Users\\42139\\Desktop\\HTMLFile\\";
    //文件路径加名称
    private static String filenameTemp;

    /**
     * 创建文件
     * @return
     */
    public static boolean createFile(String fileName,String filecontent,Integer num){
        Boolean bool = false;
        filenameTemp = filepath+fileName;
        File file = new File(filenameTemp);
        try {
            file.createNewFile();
            bool = true;
            System.out.println("成功创建网页文件"+filenameTemp);
            //将内容追加到网页文件
            writeContent(filenameTemp,filecontent,num);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return true;
    }

    public static boolean writeContent(String filepath, String newStr,Integer num) throws IOException {
        Boolean bool = false;


        String temp = "";
        FileInputStream fis = null;
        InputStreamReader isr = null;
        BufferedReader br = null;
        FileOutputStream fos = null;
        PrintWriter pw = null;
        File file = new File(filepath);
        try {
            fis = new FileInputStream(file);
            isr = new InputStreamReader(fis);
            br = new BufferedReader(isr);
            fos = new FileOutputStream(file);
            pw = new PrintWriter(fos);
            pw.write(newStr.toCharArray());
            bool = true;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }finally {
            if (pw != null){
                pw.close();
            }if (fos!=null){
                fos.close();
            }if (br != null){
                br.close();
            }if (isr != null){
                isr.close();
            }if (fis != null){
                fis.close();
            }
        }
        return bool;
    }

    public static void main(String args[]){
        String contentHTML = null;
        String fileName = null;
        for (int i = 1; i< 100; i++ ){
            contentHTML = "<html><title>No."+ i +"网站"+"</title><body><h1>欢迎来到第"+i+"个网站</h1></body></html>";
            fileName = "No."+i+".html";
            createFile(filepath,contentHTML,i);
        }
    }
}
