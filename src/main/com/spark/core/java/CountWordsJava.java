package com.spark.core.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

/**
 * CountWordsJava
 * Created by TZQ on 2016/12/9.
 */
public class CountWordsJava {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("CountWordsJava").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> words = sc.textFile("c:\\spark\\spark.txt", 1);

        JavaRDD<Integer> map = words.map(new Function<String, Integer>() {
            public Integer call(String s) throws Exception {
                return s.length();
            }
        });
        int wordsTotal = map.reduce(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        System.out.println("words Total:"+wordsTotal);
        sc.close();
    }
}
