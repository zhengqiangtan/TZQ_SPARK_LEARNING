package com.spark.core.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;
import scala.actors.threadpool.Arrays;

import java.util.Iterator;

/**
 * Created by TZQ on 2016/12/9.
 */
public class TopNGroup {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rdd = sc.textFile("c:\\spark\\score.txt", 1);

        JavaPairRDD<String, Integer> pairRDD = rdd.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) throws Exception {
                String[] split = s.split(" ");

                return new Tuple2<String, Integer>(String.valueOf(split[0]), Integer.parseInt(split[1]));
            }
        });

        JavaPairRDD<String, Iterable<Integer>> groupByKey = pairRDD.groupByKey();

        JavaPairRDD<String, Iterable<Integer>> top3PairRdd = groupByKey.mapToPair(new PairFunction<Tuple2<String, Iterable<Integer>>, String, Iterable<Integer>>() {
            public Tuple2<String, Iterable<Integer>> call(Tuple2<String, Iterable<Integer>> classScores) throws Exception {
                //###分组取前三算法###
                //思路：先取出前三个值放入数组，将后面的数据和前三个依次比较，若后者大于前者则替换，原数组位置后移。
                Integer[] top3 = new Integer[3];
                String className = classScores._1;
                Iterator<Integer> scores = classScores._2.iterator();
                while (scores.hasNext()) {
                    Integer score = scores.next();
                    for (int i = 0; i < 3; i++) {
                        if (top3[i] == null) {
                            top3[i] = score;
                            break;
                        } else if (score > top3[i]) {
                            for (int j = 2; j > i; j--) {
                                top3[j] = top3[j - 1];
                            }
                            top3[i] = score;
                            break;
                        }

                    }
                }

                return new Tuple2<String, Iterable<Integer>>(className, Arrays.asList(top3));
            }
        });


        top3PairRdd.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
            public void call(Tuple2<String, Iterable<Integer>> t) throws Exception {
                String className = t._1;
                System.out.println("class:"+className);
                Iterator<Integer> scores = t._2.iterator();
                while (scores.hasNext()){
                    System.out.println(scores.next());
                }
                System.out.println("------------------------------");
            }
        });
    }
}

//class:class1
//        95
//        90
//        87
//        ------------------------------
//class:class2
//        88
//        87
//        77
//        ------------------------------