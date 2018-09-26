package com.changxiaoshan;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

public class SparkWordCount {
    public static void main(String[] args)
    {
        String inputFile =args[0];
        //"hdfs://vbsearch/tmp/zhenjun1/oridata/20180805.txt.filter"; 
        String outputFile = args[1];
        //"hdfs://vbsearch/tmp/xiaoshan7/spark2";

        SparkConf conf = new SparkConf().setMaster("yarn");
        conf.setAppName("SparkWordCount");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> input = sc.textFile(inputFile);

        JavaRDD<String> words = input.flatMap(
                new FlatMapFunction<String, String>() {
                    public Iterator<String> call(String x) {
                        return Arrays.asList(x.split(" ")).iterator();
                    }});

        JavaPairRDD<String, Integer> counts = words.mapToPair(
                new PairFunction<String, String, Integer>(){
                    public Tuple2<String, Integer> call(String x){
                        return new Tuple2(x, 1);
                    }}).reduceByKey(new Function2<Integer, Integer, Integer>(){
            public Integer call(Integer x, Integer y){ return x + y;}});

        counts.saveAsTextFile(outputFile);
        sc.stop();
    }
}
