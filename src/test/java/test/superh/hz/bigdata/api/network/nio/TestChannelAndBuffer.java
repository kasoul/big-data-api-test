package test.superh.hz.bigdata.api.network.nio;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class TestChannelAndBuffer {

	public static void main(String[] args) {
		try {
			testFileChannelWriteFile();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void testFileChannelReadFile() throws IOException {
		RandomAccessFile aFile = new RandomAccessFile("C:\\Users\\Administrator\\Desktop\\testdata\\test-io.txt", "rw");
		FileChannel inChannel = aFile.getChannel();

		ByteBuffer buf = ByteBuffer.allocate(100);
		byte[] dst = new byte[100];
		int dst_index = 0 ;
		
		int bytesRead = inChannel.read(buf);
		while (bytesRead != -1) {

			System.out.println("Read " + bytesRead);
			buf.flip();

			while (buf.hasRemaining()) {
				//byte[] dst = new byte[2];
				dst[dst_index++] = buf.get();
			}

			buf.clear();
			bytesRead = inChannel.read(buf);
		}
		aFile.close();
		
		System.out.print(new String(dst,"GBK"));
		
	}
	
	public static void testFileChannelWriteFile() throws IOException {
		RandomAccessFile aFile = new RandomAccessFile("C:\\Users\\Administrator\\Desktop\\testdata\\test-io2.txt", "rw");
		FileChannel inChannel = aFile.getChannel();

		ByteBuffer buf = ByteBuffer.allocate(48);
		byte[] src = "ccc\nddd\n外国人".getBytes("gbk");
		for(byte b:src){
			buf.put(b);
		}
		
		buf.flip();
		
		int bytesWrite = inChannel.write(buf);
		System.out.println("Write " + bytesWrite);
		
		aFile.close();

		
	}
}
