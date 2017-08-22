package Moudle.Pool;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by cluster on 2017/5/3.
 */
public class DataStruts {
    public static void main(String args[]){
        List<String> list = new ArrayList<String>();
        list.add("cong");
        list.add("pei");
        list.add("xin");

        //第一种遍历方法使用foreach遍历List
//        for (String a : list){
//            System.out.println(a);
//        }

        //转换成数组进行遍历
//        String[] listArray = new String[list.size()];
//        list.toArray(listArray);
//        for (String b : listArray){
//            System.out.println(b);
//        }

        //迭代器
//        Iterator<String> it = list.iterator();
//        while(it.hasNext())//判断下一个元素之后有值
//        {
//            System.out.println(it.next());
//        }

        Map<Integer,String> map = new HashMap<Integer,String>();
        map.put(1,"cong");
        map.put(2,"pei");
        map.put(3,"xin");

//        for (Integer a : map.keySet()){
//            System.out.println(a+":"+map.get(a));
//        }


//        for (Map.Entry<Integer,String> m :map.entrySet()){
//            System.out.println(m.getKey()+":"+m.getValue());
//        }




    }
}
