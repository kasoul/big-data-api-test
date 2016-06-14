package com.superh.hz.bigdata.api.hdfs.client;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IOUtils;

/** 
 *
 * @Project: big-apple-api-test
 * @File: HDFSClient.java 
 * @Date: 2015年4月8日 
 * @Author: 黄超（huangchaohz）
 */

/**
 * @Describe:
 */
public class HDFSClient {

	public static void main(String args[]) {
		/*
		 * System.out.println(System.getProperty("user.name"));
		 * System.out.println(System.getenv("HADOOP_HOME"));
		 * System.out.println(System.getenv("HADOOP_USER_NAME"));
		 */
		/*
		 * Map<String, String> m = System.getenv(); for (Iterator<String> it =
		 * m.keySet().iterator(); it.hasNext(); ) { String key = (String )
		 * it.next(); String value = (String ) m.get(key);
		 * System.out.println(key +":" +value); } System.out.println(
		 * "--------------------------------------" );
		 */
		//String path = "/test/hdfstesthc/test1.dat";
		//writeFile(path);
		//listFiles("/test/hdfstesthc");
		listFiles("/");
		//listFiles("/user/hive/warehouse/2015052613_test2_b893ebd2b91c4708a31fe6cde72f7a7d");
		//readFile("/user/hive/warehouse/2015052613_test2_bb66e140d44e411f983b38731dd8af38/part-m-00000_copy_1");
	}

	public static void listFiles(String hdfsPath) {
		try {
			Configuration config = new Configuration();
			FileSystem hdfs = FileSystem.get(config);
			Path path = new Path(hdfsPath);
			// 列出目录下的子文件，不列出子目录
			/*
			 * RemoteIterator<LocatedFileStatus> files =
			 * hdfs.listFiles(path,false); while(files.hasNext()){
			 * LocatedFileStatus lfs = files.next();
			 * System.out.println(lfs.getPath()); }
			 */
			// 列出目录下的所有子项目
			FileStatus[] files2 = hdfs.listStatus(path);
			for (FileStatus fstatus : files2) {
				System.out.println(fstatus.getPath());
			}
			hdfs.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * 读取文件，原来文件不存在报异常
	 * 
	 * @param hdfsPath
	 *            格式为hdfs://ip:port/destination
	 */
	public static void readFile(String hdfsPath) {
		try {
			Configuration config = new Configuration();
			FileSystem hdfs = FileSystem.get(config);
			Path path = new Path(hdfsPath);
			FSDataInputStream fsin = hdfs.open(path);
			BufferedReader bis = new BufferedReader(new InputStreamReader(fsin));
			String temp;
			int linecount = 0;
			while ((temp = bis.readLine()) != null) {
				System.out.println(temp);
				linecount++;
			}
			System.out.println(linecount);
			bis.close();
			fsin.close();
			hdfs.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 写入文件，原来文件不存在自动创建文件
	 * 
	 * @param hdfsPath
	 *            格式为hdfs://ip:port/destination
	 */
	public static void writeFile(String hdfsPath) {
		try {
			Configuration config = new Configuration();
			FileSystem hdfs = FileSystem.get(config);
			Path path = new Path(hdfsPath);
			FSDataOutputStream fsout = hdfs.create(path);
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fsout));
			StringBuffer sb = new StringBuffer();
			sb.append("4,hechan,hechan");
			sb.append("\n");
			sb.append("5,linqingyou,linqingyou");
			sb.append("\n");
			bw.write(sb.toString());
			bw.flush();
			bw.close();
			hdfs.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 上传文件，
	 * 
	 * @param localFile
	 *            本地路径
	 * @param hdfsPath
	 *            格式为hdfs://ip:port/destination
	 * @throws IOException
	 */
	public static void upFile(String localFile, String hdfsPath) throws IOException {
		Configuration config = new Configuration();
		FileSystem hdfs = FileSystem.get(config);
		InputStream in = new BufferedInputStream(new FileInputStream(localFile));
		OutputStream out = hdfs.create(new Path(hdfsPath));
		IOUtils.copyBytes(in, out, config);
	}

	/**
	 * 附加文件
	 * 
	 * @param localFile
	 * @param hdfsPath
	 * @throws IOException
	 */
	public static void appendFile(String localFile, String hdfsPath) throws IOException {
		Configuration config = new Configuration();
		FileSystem hdfs = FileSystem.get(config);
		InputStream in = new FileInputStream(localFile);
		OutputStream out = hdfs.append(new Path(hdfsPath));
		IOUtils.copyBytes(in, out, config);
	}

	/**
	 * 下载文件
	 * 
	 * @param hdfsPath
	 * @param localPath
	 * @throws IOException
	 */
	public static void downFile(String hdfsPath, String localPath) throws IOException {
		Configuration config = new Configuration();
		FileSystem hdfs = FileSystem.get(config);
		InputStream in = hdfs.open(new Path(hdfsPath));
		OutputStream out = new FileOutputStream(localPath);
		IOUtils.copyBytes(in, out, config);
	}

	/**
	 * 删除文件或目录
	 * 
	 * @param hdfsPath
	 * @throws IOException
	 */
	public static void delFile(String hdfsPath) {
		Configuration config = new Configuration();
		FileSystem hdfs;
		try {
			hdfs = FileSystem.get(config);
			hdfs.delete(new Path(hdfsPath), true);
			hdfs.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
