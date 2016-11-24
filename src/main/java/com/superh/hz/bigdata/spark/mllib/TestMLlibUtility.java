package com.superh.hz.bigdata.spark.mllib;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.random.RandomRDDs;

/**
 * Created by Administrator on 2016/7/13.
 */
public class TestMLlibUtility {

    public static void main(String[] args) {
        testRandomData();
    }

    //随机数生产器
    public static void testRandomData(){

        SparkConf conf = new SparkConf().setAppName("Simple Application");
        conf.setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //生成100个随机数，N（0,1）标准正态分布，均匀分布在2个partition中
        JavaDoubleRDD n = RandomRDDs.normalJavaRDD(sc, 100L, 1);
        //生成100个随机数，期望和方差为10的泊松分布
        JavaDoubleRDD p = RandomRDDs.poissonJavaRDD(sc, 10, 100L);
        //生成100个随机数，0～1上的连续型均匀分布
        JavaDoubleRDD u = RandomRDDs.uniformJavaRDD(sc, 100);

        //将标准正态分布N（0,1）变成N（1,4）
       /* JavaDoubleRDD v = n.mapToDouble(x -> 1.0 + 2.0 * x);
        for(Double d : v.collect()) {
            System.out.println(d);
        }*/
       /* for(Double d : p.collect()) {
            System.out.println(d);
        }*/
        //将0～1均匀分布变为2～5均匀分布
        JavaDoubleRDD g = u.mapToDouble(x -> 2.0 + 3.0 * x);
        for(Double d : g.collect()) {
            System.out.println(d);
        }
    }
}
