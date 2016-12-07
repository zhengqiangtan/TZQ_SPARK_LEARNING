package com.spark.core.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;

/**
 * 案例一：统计单词频次并按照从高到低顺序输出
 * Created by TZQ on 2016/12/7.
 */
public class WordCountJava {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("WordCountJava").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("c:\\spark.txt", 1);
        //1. flatMap将单词按照空格打散
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" "));
            }
        });
        //2.将打散的单词按照（word,1）进行标记
        JavaPairRDD<String, Integer> wordPairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });
        //3.计算单词出现的总次数（word,7）
        JavaPairRDD<String, Integer> wordcount = wordPairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        //4.将上面的输出结果中的key和value互换位置
        JavaPairRDD<Integer, String> countWords = wordcount.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            public Tuple2<Integer, String> call(Tuple2<String, Integer> t) throws Exception {
                return new Tuple2<Integer, String>(t._2,t._1);
            }
        });
        //5.按照value值从高到低进行排序
        JavaPairRDD<Integer, String> sortWords = countWords.sortByKey(false, 1);

        JavaPairRDD<String, Integer> wordcounts = sortWords.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            public Tuple2<String, Integer> call(Tuple2<Integer, String> t) throws Exception {
                return new Tuple2<String, Integer>(t._2, t._1);
            }
        });
        //6.使用foreach算子进行输出
        wordcounts.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            public void call(Tuple2<String, Integer> t) throws Exception {
                System.out.println("单词："+t._1+" 词频："+t._2);
            }
        });

    }
}


/*
单词：of 词频：10
单词：Spark 词频：10
单词：to 词频：10
单词：the 词频：7
单词： 词频：7
单词：and 词频：7
单词：our 词频：5*/
