package com.superh.hz.bigdata.spark.mllib;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Matrices;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.stat.KernelDensity;
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary;
import org.apache.spark.mllib.stat.Statistics;
import org.apache.spark.mllib.stat.test.ChiSqTestResult;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Administrator on 2016/7/13.
 */
public class TestBasicStatistics {

    public static void main(String[] args) {
        // testSummarStatistics();
        // testCorrelations();
        //testStratifiedSampling();
        //testHypothesisTesting();
        testKernelDensityEstimation();
    }

    //主要指标统计
    public static void testSummarStatistics(){
        Vector dv1 = Vectors.dense(1.0, 0.0, 3.0);
        Vector dv2 = Vectors.dense(1.0, 0.0, 3.0);
        Vector dv3 = Vectors.dense(1.0, 0.0, 3.0);
        List<Vector> list = new ArrayList<Vector>();
        list.add(dv1);
        list.add(dv2);
        list.add(dv3);

        SparkConf conf = new SparkConf().setAppName("Simple Application");
        conf.setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Vector> rows = sc.parallelize(list); // a JavaRDD of local vectors

        // Compute column summary statistics.
        MultivariateStatisticalSummary summary = Statistics.colStats(rows.rdd());
        System.out.println(summary.mean()); // a dense vector containing the mean value for each column
        System.out.println(summary.variance()); // column-wise variance
        System.out.println(summary.numNonzeros()); // number of nonzeros in each column

    }

    //相关度
    public static void testCorrelations(){

        SparkConf conf = new SparkConf().setAppName("Simple Application");
        conf.setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Double> listX = new ArrayList<Double>();
        listX.add(1.2);
        listX.add(2.3);
        listX.add(3.2);
        List<Double> listY = new ArrayList<Double>();
        listY.add(1.2);
        listY.add(2.3);
        listY.add(3.2);
        JavaDoubleRDD seriesX = sc.parallelizeDoubles(listX); // a series
        JavaDoubleRDD seriesY = sc.parallelizeDoubles(listY); // must have the same number of partitions and cardinality as seriesX

        // compute the correlation using Pearson's method. Enter "spearman" for Spearman's method. If a
        // method is not specified, Pearson's method will be used by default.
        Double correlation = Statistics.corr(seriesX.srdd(), seriesY.srdd(), "spearman");
        System.out.println(correlation);


        Vector dv1 = Vectors.dense(1.0, 0.0, 3.0 ,2.0);
        Vector dv2 = Vectors.dense(1.1, 0.1, 3.1 ,2.1);
        Vector dv3 = Vectors.dense(1.2, 0.2, 3.2 ,2.2);
        Vector dv4 = Vectors.dense(1.3, 0.3, 3.3 ,2.3);
        List<Vector> list = new ArrayList<Vector>();
        list.add(dv1);
        list.add(dv2);
        list.add(dv3);
        list.add(dv4);
        JavaRDD<Vector> data = sc.parallelize(list); // note that each Vector is a row and not a column

        // 计算属性相关度，即列相关度
        // calculate the correlation matrix using Pearson's method. Use "spearman" for Spearman's method.
        // If a method is not specified, Pearson's method will be used by default.
        Matrix correlMatrix = Statistics.corr(data.rdd(), "spearman");
        System.out.println(correlMatrix);
    }

    //分层抽样
    public static void testStratifiedSampling(){
        SparkConf conf = new SparkConf().setAppName("Simple Application");
        conf.setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        Vector dv1 = Vectors.dense(1.0, 0.0, 3.0 ,2.0);
        Vector dv2 = Vectors.dense(1.1, 0.1, 3.1 ,2.1);
        Vector dv3 = Vectors.dense(1.2, 0.2, 3.2 ,2.2);
        Vector dv4 = Vectors.dense(1.3, 0.3, 3.3 ,2.3);
        List<Vector> list = new ArrayList<Vector>();
        list.add(dv1);
        list.add(dv2);
        list.add(dv3);
        list.add(dv4);
        JavaRDD<Vector> dataVector = sc.parallelize(list);

        // 向量非零个数作为key，向量本身作为value
        JavaPairRDD<Integer,Vector> data = dataVector.mapToPair(x -> new Tuple2(x.numNonzeros(),x));
        Map<Integer, Object> fractions = new HashMap<>(); // specify the exact fraction desired from each key
        fractions.put(3,0.1);
        fractions.put(4,0.5);

        // Get an exact sample from each stratum
        /*JavaPairRDD<Integer,Vector> approxSample = data.sampleByKey(false, fractions);
        Map<Integer,Vector> approxSampleMap = approxSample.collectAsMap();
        System.out.println(approxSampleMap.size());
        for(int i :approxSampleMap.keySet()){
            System.out.println(approxSampleMap.get(i));
        }*/

        JavaPairRDD<Integer,Vector> exactSample = data.sampleByKeyExact(false, fractions);
        Map<Integer,Vector> exactSampleMap = exactSample.collectAsMap();
        System.out.println(exactSampleMap.size());
        for(int i :exactSampleMap.keySet()){
            System.out.println(exactSampleMap.get(i));
        }
    }

    //假设检验
    public static void testHypothesisTesting(){
        SparkConf conf = new SparkConf().setAppName("Simple Application");
        conf.setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        Vector dv_real = Vectors.dense(1.0, 0.0, 3.0 ,2.0);
        Vector dv_expected = Vectors.dense(1.1, 0.1, 3.0 ,2.2);
        // a vector composed of the frequencies of events
        // ChiSqTestResult goodnessOfFitTestResult = Statistics.chiSqTest(dv_real,dv_expected);

        // 在计算适应度的时候，如果只有实际值而没有期望值，默认期望值为均匀分布
        ChiSqTestResult goodnessOfFitTestResult = Statistics.chiSqTest(dv_real);

        // summary of the test including the p-value, degrees of freedom, test statistic, the method used,
        // and the null hypothesis.
        System.out.println(goodnessOfFitTestResult);

        Matrix dm = Matrices.dense(3, 3, new double[] {1.0, 3.0, 5.0, 2.0, 4.0, 6.0,1.5,3.5,5.5});

        // conduct Pearson's independence test on the input contingency matrix
        ChiSqTestResult independenceTestResult = Statistics.chiSqTest(dm);
        // summary of the test including the p-value, degrees of freedom...
        System.out.println(independenceTestResult);

        //  JavaRDD<LabeledPoint> obs = ... // an RDD of labeled points

        // The contingency table is constructed from the raw (feature, label) pairs and used to conduct
        // the independence test. Returns an array containing the ChiSquaredTestResult for every feature
        // against the label.
        //  ChiSqTestResult[] featureTestResults = Statistics.chiSqTest(obs.rdd());
        //  int i = 1;
        /*//  for (ChiSqTestResult result : featureTestResults) {
            System.out.println("Column " + i + ":");
            System.out.println(result); // summary of the test
            i++;
        }*/
    }

    //核密度计算
    public static void testKernelDensityEstimation(){

        List<Double> dataList = new ArrayList<Double>();
        dataList.add(1.2);
        dataList.add(2.3);
        dataList.add(3.2);

        SparkConf conf = new SparkConf().setAppName("Simple Application");
        conf.setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Double> data =  sc.parallelize(dataList);
        // Construct the density estimator with the sample data and a standard deviation for the Gaussian
        // kernels
        KernelDensity kd = new KernelDensity()
                .setSample(data)
                .setBandwidth(3.0);

        // Find density estimates for the given values
        double[] densities = kd.estimate(new double[] {-1.0, 2.0, 5.0});
        for(double d : densities) {
            System.out.println(d);
        }

    }
}
