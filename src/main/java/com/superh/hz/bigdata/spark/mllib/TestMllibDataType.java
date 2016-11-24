package com.superh.hz.bigdata.spark.mllib;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.mllib.linalg.*;
import org.apache.spark.mllib.linalg.distributed.*;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;
import scala.Function1;
import scala.Tuple2;

import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Administrator on 2016/7/12.
 */
public class TestMllibDataType {

    public static void main(String[] args) {
        //testLocalVector();
        //testLabeledPoint();
        //testLIBSVMData();
        //testLacalMatrix();
        //testRowMatrix();
        //testIndexedRowMatrix();
        testCoordinateMatrix();

    }

    public static void testLocalVector(){
        // Create a dense vector (1.0, 0.0, 3.0).
        Vector dv = Vectors.dense(1.0, 0.0, 3.0);
        // Create a sparse vector (1.0, 0.0, 3.0) by specifying its indices and values corresponding to nonzero entries.
        Vector sv = Vectors.sparse(3, new int[] {0, 2}, new double[] {1.0, 3.0});

        System.out.println(dv);
        System.out.println(sv);
    }

    public static void testLabeledPoint(){
        // Create a labeled point with a positive label and a dense feature vector.
        LabeledPoint pos = new LabeledPoint(1.0, Vectors.dense(1.0, 0.0, 3.0));

        // Create a labeled point with a negative label and a sparse feature vector.
        LabeledPoint neg = new LabeledPoint(0.0, Vectors.sparse(3, new int[] {0, 2}, new double[] {1.0, 3.0}));

        System.out.println(pos);
        System.out.println(neg);

    }

    public static void testLIBSVMData(){
        String classpath = "";
        try {
            classpath = TestMllibDataType.class.getClassLoader().getResource("").toURI().getPath();
        } catch (URISyntaxException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        String logFile = "file://" + classpath + "a1a.t"; // Should be some file on
        SparkConf conf = new SparkConf().setAppName("Simple Application");
        conf.setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<LabeledPoint> examples =
                MLUtils.loadLibSVMFile(sc.sc(), logFile).toJavaRDD();
        List<LabeledPoint> list = examples.collect();
        for(LabeledPoint lp:list){
            System.out.println(lp);
        }
    }

    public static void testLacalMatrix(){
        // Create a dense matrix ((1.0, 2.0), (3.0, 4.0), (5.0, 6.0))
        //     1.0, 2.0
        //     3.0, 4.0
        //     5.0, 6.0
        Matrix dm = Matrices.dense(3, 2, new double[] {1.0, 3.0, 5.0, 2.0, 4.0, 6.0});
        System.out.println(dm);

        // Create a sparse matrix ((9.0, 0.0), (0.0, 8.0), (0.0, 6.0))
        //     9.0, 0.0
        //     0.0, 8.0
        //     0.0, 6.0
        Matrix sm = Matrices.sparse(3, 2, new int[] {0, 1, 3}, new int[] {0, 2, 1}, new double[] {9, 6, 8});
        System.out.println(sm);

    }

    public static void testRowMatrix(){
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
        // Create a RowMatrix from an JavaRDD<Vector>.
        RowMatrix mat = new RowMatrix(rows.rdd());

        // Get its size.
        long m = mat.numRows();
        long n = mat.numCols();
        System.out.println(m+","+n);

        // QR decomposition
        QRDecomposition<RowMatrix, Matrix> result = mat.tallSkinnyQR(true);
        result.Q();
        result.R();

    }

    public static void testIndexedRowMatrix(){
        Vector dv1 = Vectors.dense(1.0, 0.0, 3.0);
        Vector dv2 = Vectors.dense(1.0, 0.0, 3.0);
        Vector dv3 = Vectors.dense(1.0, 0.0, 3.0);
        IndexedRow ir1 = new IndexedRow(0,dv1);
        IndexedRow ir2 = new IndexedRow(1,dv1);
        IndexedRow ir3 = new IndexedRow(2,dv1);
        List<IndexedRow> list = new ArrayList<>();
        list.add(ir1);
        list.add(ir2);
        list.add(ir3);

        SparkConf conf = new SparkConf().setAppName("Simple Application");
        conf.setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<IndexedRow> rows = sc.parallelize(list); // a JavaRDD of local vectors

        IndexedRowMatrix imat = new IndexedRowMatrix(rows.rdd());

        // Get its size.
        long m = imat.numRows();
        long n = imat.numCols();
        System.out.println(m + "," + n);

        // Drop its row indices.
        RowMatrix rowMat = imat.toRowMatrix();
        System.out.println(rowMat);
    }

    public static void testCoordinateMatrix(){
        MatrixEntry me1 = new MatrixEntry(0,0,2.7);
        MatrixEntry me2 = new MatrixEntry(4,5,3.5);
        MatrixEntry me3 = new MatrixEntry(4,7,4.3);
        List<MatrixEntry> list = new ArrayList<>();
        list.add(me1);
        list.add(me2);
        list.add(me3);

        SparkConf conf = new SparkConf().setAppName("Simple Application");
        conf.setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<MatrixEntry> entries = sc.parallelize(list); // a JavaRDD of matrix entries
        // Create a CoordinateMatrix from a JavaRDD<MatrixEntry>.
        CoordinateMatrix coordMat = new CoordinateMatrix(entries.rdd());

        // Get its size.
        long m = coordMat.numRows();
        long n = coordMat.numCols();
        System.out.println(m + "," + n);

        // Convert it to an IndexRowMatrix whose rows are sparse vectors.
        IndexedRowMatrix indexedRowMatrix = coordMat.toIndexedRowMatrix();

        // Transform the CoordinateMatrix to a BlockMatrix
        BlockMatrix matA = coordMat.toBlockMatrix(2,2).cache();
        System.out.println(matA.numRowBlocks());
        System.out.println(matA.numRows());
        System.out.println(matA.numColBlocks());
        System.out.println(matA.numCols());
        System.out.println(matA.toLocalMatrix());
        System.out.println("-------------------I am split line---------------------");

        // Validate whether the BlockMatrix is set up properly. Throws an Exception when it is not valid.
        // Nothing happens if it is valid.
        matA.validate();

        // Calculate A^T A.
        BlockMatrix ata = matA.transpose().multiply(matA);
        System.out.println(ata.toLocalMatrix());
    }

}
