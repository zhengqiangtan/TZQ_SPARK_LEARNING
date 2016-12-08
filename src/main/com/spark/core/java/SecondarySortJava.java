package com.spark.core.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

/**
 * 案例二：二次排序
 * 需求：
 * 1、按照文件中的第一列排序。
 * 2、如果第一列相同，则按照第二列排序。
 *
 * 实现思路：
 * 1、实现自定义的key，要实现Ordered接口和Serializable接口，在key中实现自己对多个列的排序算法
 * 2、将包含文本的RDD，映射成key为自定义key，value为文本的JavaPairRDD
 * 3、使用sortByKey算子按照自定义的key进行排序
 * 4、再次映射，剔除自定义的key，只保留文本行
 * Created by TZQ on 2016/12/7.
 */
public class SecondarySortJava {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("SecondarySortJava").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);


        JavaRDD<String> linesRdd = sc.textFile("c:\\sort.txt", 1);
        //1.产生新的组合《自定义排序key,<key,value>》
        JavaPairRDD<SecondarySortKey, String> pairRDD = linesRdd.mapToPair(new PairFunction<String, SecondarySortKey, String>() {
            public Tuple2<SecondarySortKey, String> call(String line) throws Exception {
                String[] split = line.split(" ");
                SecondarySortKey key = new SecondarySortKey(Integer.valueOf(split[0]), Integer.valueOf(split[1]));
                return new Tuple2<SecondarySortKey, String>(key,line);
            }
        });


        //2.根据自定义可以进行排序
        JavaPairRDD<SecondarySortKey, String> sortByKey = pairRDD.sortByKey();

        //3.去除自定义key得到<key,value>
        JavaRDD<String> map = sortByKey.map(new Function<Tuple2<SecondarySortKey, String>, String>() {
            public String call(Tuple2<SecondarySortKey, String> t) throws Exception {
                return t._2;
            }
        });

        //4.遍历输出
        map.foreach(new VoidFunction<String>() {
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });

        sc.close();

    }
}

//  1 5        1 3
//  2 4        1 5
//  3 6   ->   2 1
//  1 3        2 4
//  2 1        3 6