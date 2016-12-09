package com.spark.core.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.List;

/**
 * 对文本文件内的数字，取最大的前3个
 * Created by TZQ on 2016/12/9.
 */
public class Top3 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Top3").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rdd = sc.textFile("c:\\spark\\top.txt", 1);

        JavaPairRDD<Integer, String> pairRDD = rdd.mapToPair(new PairFunction<String, Integer, String>() {
            public Tuple2<Integer, String> call(String s) throws Exception {
                return new Tuple2<Integer, String>(Integer.parseInt(s), s);
            }
        });

        JavaPairRDD<Integer, String> sortByKey = pairRDD.sortByKey(false, 1);


        List<Tuple2<Integer, String>> top3 =  sortByKey.take(3);

        for (Tuple2<Integer, String> t:top3){
//            System.out.println(t);
//            (9,9)
//            (7,7)
//            (6,6)
            System.out.println(t._1());
        }

        sc.close();
    }
}
