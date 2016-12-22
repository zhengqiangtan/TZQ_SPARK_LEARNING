package com.spark.core.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;


/**
 * 转换算子相关实例演练
 * Created by TZQ on 2016/12/14.
 */
public class TransformationOperation {
    static SparkConf conf = new SparkConf().setAppName("").setMaster("local");
    static JavaSparkContext sc = new JavaSparkContext(conf);
    static List<Integer> list= Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    static JavaRDD<Integer> rdd = sc.parallelize(list);

    public static void main(String[] args) {
        map();

        sc.close();
    }


    /**
     * map算子案例：将集合中每一个元素都乘以2
     */
    private static void map(){
        JavaRDD<Integer> map = rdd.map(new Function<Integer, Integer>() {
            public Integer call(Integer v1) throws Exception {
                return v1 * 2;
            }
        });
        map.foreach(new VoidFunction<Integer>() {
            public void call(Integer t) throws Exception {
                System.out.println("每个元素乘以2："+t);
            }
        });
    }

    /**
     * filter算子案例：过滤集合中的偶数
     */
    private static void filter() {
        JavaRDD<Integer> rdd2 =rdd.filter(new Function<Integer, Boolean>() {
            public Boolean call(Integer v1)throws Exception {
                return v1 % 2 == 0;
            }
        });

        rdd2.foreach(new VoidFunction<Integer>() {
            public void call(Integer t)throws Exception {
                System.out.println("过滤后的偶数："+ t);
            }
        });
    }


    /**
     * flatMap案例：将文本行拆分为多个单词
     */
    private static void flatMap() {
        List<String> list1 = Arrays. asList("hello tan","hello world", "hello tan");

        JavaRDD<String> rdd2 =sc.parallelize(list1);
        JavaRDD<String> rdd3 = rdd2.flatMap(new FlatMapFunction<String, String>() {
                    public Iterable<String> call(String t)throws Exception {
                        return Arrays.asList(t.split(" "));
                    }
                });

        rdd3.foreach(new VoidFunction<String>() {
            public void call(String t) throws Exception {
                System.out.println("拆分的单词："+ t);

            }
        });

    }
    /**
     * groupByKey案例：按照班级对成绩进行分组 groupByKey算子，返回的还是JavaPairRDD
     */
    @SuppressWarnings("unchecked")
    private static void groupByKey() {

        List<Tuple2<String, Integer>> score = Arrays.asList(
        new Tuple2<String, Integer>("class1", 80),
        new Tuple2<String, Integer>("class2", 75),
        new Tuple2<String, Integer>("class1", 90),
        new Tuple2<String, Integer>("class2", 65));

        JavaPairRDD<String, Integer> rdd2 =sc.parallelizePairs(score);
        JavaPairRDD<String, Iterable<Integer>> rdd3 = rdd2.groupByKey();

        // rdd3.foreach (new VoidFunction<Tuple2<String,Iterable<Integer>>>() {
        // public void call(Tuple2<String, Iterable<Integer>> t)
        // throws Exception {
        // System.out.println("class:"+t._1);
        //
        // Iterator<Integer> iterator = t._2.iterator();
        // while(iterator.hasNext()){
        // System.out.println(iterator.next());
        // System.out.println("-------------------------");
        //
        // }
        //
        //
        // }
        // });

        List<Tuple2<String, Iterable<Integer>>> collect = rdd3.collect();
        for(Tuple2<String, Iterable<Integer>> tuple2 : collect) {
            System.out.println(tuple2);
            System.out.println(tuple2.getClass() +"  "+ tuple2._1+"  "
                    + tuple2._2);

            Iterator<Integer> ite = tuple2._2.iterator();
            while(ite.hasNext()) {
                System.out.println("遍历Tuple2:"+ ite.next());
            }

            // (class1,[80, 90])
            // class scala.Tuple2 class1 [80, 90]
            // 遍历Tuple2:80
            // 遍历Tuple2:90
            // (class2,[75, 65])
            // class scala.Tuple2 class2 [75, 65]
            // 遍历Tuple2:75
            // 遍历Tuple2:65
        }

    }



    /**
     * reduceByKey案例：统计每个班级的总分
     *
     * output: class1-->170 class2-->140
     */
    @SuppressWarnings({"unchecked","serial"})
    private static void reduceByKey() {
        // 模拟集合
        List<Tuple2<String, Integer>> scoreList = Arrays.asList(
        new Tuple2<String, Integer>("class1", 80),
        new Tuple2<String, Integer>("class2", 75),
        new Tuple2<String, Integer>("class1", 90),
        new Tuple2<String, Integer>("class2", 65));

        // 并行化集合，创建JavaPairRDD
        JavaPairRDD<String, Integer> scores =sc.parallelizePairs(scoreList);
        JavaPairRDD<String, Integer> rdd2 = scores
                .reduceByKey(new Function2<Integer, Integer, Integer>() {

            public Integer call(Integer v1, Integer v2)
            throws Exception {
                return v1 + v2;
            }
        });
        rdd2.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            public void call(Tuple2<String, Integer> t)throws Exception {
                System.out.println(t._1+"-->"+ t._2);
            }
        });

        sc.close();

    }
    /**
     * sortByKey案例：按照学生分数进行排序
     *  anna's score: 90
     tom's score: 80
     andy's score: 75
     john's score: 65
     */
    @SuppressWarnings({"unchecked","unused","serial"})
    private static void sortByKey() {
        // 模拟集合
        List<Tuple2<String, Integer>> scoreList = Arrays.asList(
        new Tuple2<String, Integer>("tom", 80),
        new Tuple2<String, Integer>("andy", 75),
        new Tuple2<String, Integer>("anna", 90),
        new Tuple2<String, Integer>("john", 65));

        // 并行化集合，创建RDD
        JavaPairRDD<String, Integer> rdd2 =sc.parallelizePairs(scoreList);

        //此时我们可以按照value排序，逆转key,value
        JavaPairRDD<Integer, String> rdd3 = rdd2.mapToPair(new PairFunction<Tuple2<String,Integer>, Integer, String>(){
            public Tuple2<Integer, String> call(Tuple2<String, Integer> t)
            throws Exception {

                return new Tuple2<Integer, String>(t._2, t._1);
            }
        });
        // 对rdd3执行sortByKey算子
        JavaPairRDD<Integer, String> rdd4 = rdd3.sortByKey(false);

        //打印rdd4
        rdd4.foreach(new VoidFunction<Tuple2<Integer,String>>() {
            public void call(Tuple2<Integer, String> t)throws Exception {
                System.out.println(t._2+"'s score: "+t._1);

            }
        });

        sc.close();
    }


    /**
     * join案例：打印学生成绩
     *  1-->(leo,100)
     3-->(tom,60)
     2-->(jack,90)
     *
     */
    @SuppressWarnings("unchecked")
    private static void join() {
        // 模拟集合
        List<Tuple2<Integer, String>> studentList = Arrays.asList(
        new Tuple2<Integer, String>(1,"leo"),
        new Tuple2<Integer, String>(2,"jack"),
        new Tuple2<Integer, String>(3,"tom"));

        List<Tuple2<Integer, Integer>> scoreList = Arrays.asList(new Tuple2<Integer, Integer>(1, 100),
        new Tuple2<Integer, Integer>(2, 90),
        new Tuple2<Integer, Integer>(3, 60));

        JavaPairRDD<Integer, String> stuRDD =sc.parallelizePairs(studentList);
        JavaPairRDD<Integer, Integer> scoreRDD =sc.parallelizePairs(scoreList);

        //执行join算子
        JavaPairRDD<Integer, Tuple2<String, Integer>> joinPairRDD = stuRDD.join(scoreRDD);

        //按照key排序
        JavaPairRDD<Integer, Tuple2<String, Integer>> sortByKey = joinPairRDD.sortByKey(true);

        sortByKey.foreach(new VoidFunction<Tuple2<Integer,Tuple2<String,Integer>>>() {
            public void call(Tuple2<Integer, Tuple2<String, Integer>> t)
            throws Exception {
                System.out.println(t._1+"-->"+t._2);
                System.out.println("id:"+t._1+"  name:"+t._2._1+"  score:"+t._2._2);

            }
        });

    }

//   1-->( leo,100)
//   id:1  name:leo  score:100
//   2-->(jack,90)
//   id:2  name:jack  score:90
//   3-->(tom,60)
//   id:3  name:tom  score:60



    /**
     * cogroup案例：打印学生成绩（分组合并）
     */
    @SuppressWarnings({"unchecked","unused"})
    private static void cogroup() {
        // 模拟集合
        List<Tuple2<Integer, String>> studentList = Arrays.asList(new Tuple2<Integer, String>(1,"leo"),
        new Tuple2<Integer, String>(2,"jack"),
        new Tuple2<Integer, String>(3,"tom"));

        List<Tuple2<Integer, Integer>> scoreList = Arrays.asList(new Tuple2<Integer, Integer>(1, 100),
        new Tuple2<Integer, Integer>(2, 90),
        new Tuple2<Integer, Integer>(3, 60),
        new Tuple2<Integer, Integer>(1, 70),
        new Tuple2<Integer, Integer>(2, 80),
        new Tuple2<Integer, Integer>(3, 50));


        JavaPairRDD<Integer, String> stuPairRDD =sc.parallelizePairs(studentList);
        JavaPairRDD<Integer, Integer> scorePairRDD =sc.parallelizePairs(scoreList);


        JavaPairRDD<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> cogroup = stuPairRDD.cogroup(scorePairRDD);


        cogroup.foreach(new VoidFunction<Tuple2<Integer,Tuple2<Iterable<String>,Iterable<Integer>>>>() {

            public void call(
                    Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> t)
            throws Exception {
                System.out.println("id:"+t._1+"  name:"+t._2._1+"  score:"+t._2._2);

            }

        });
        sc.close();
    }

//   id:1  name:[ leo]  score:[100, 70]
//   id:3  name:[tom]  score:[60, 50]
//   id:2  name:[jack]  score:[90, 80]

//   这里如果改为Join的话，结果是：
//                id:1  name:leo  score:100
//                id:1  name:leo  score:70
//                id:3  name:tom  score:60
//                id:3  name:tom  score:50
//                id:2  name:jack  score:90
//                id:2  name:jack  score:80

}
