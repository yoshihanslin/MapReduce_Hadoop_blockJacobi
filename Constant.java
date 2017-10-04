import java.io.*;
import java.io.BufferedReader;
import java.io.FileReader;

public class Constant {

	public static boolean SHOW_PAGERANK = false; 

	public static final double EPSILON = 0.001; 
	public static final double DECIMAL_ACCURACY = 1000000.0;
	
	public static final int NUM_ALL_NODE = 685230;

	// hard code number of nodes in every block
	public static final int BLOCKNUM = 68;
	public static final int[] inBlockNum = { 
	 10328, 
	 10045,
	 10256,
	 10016,
	  9817,
	 10379,
	  9750,
	  9527,
	 10379,
	 10004,
	 10066,
	 10378,
	 10054,
	  9575,
	 10379,
	 10379,
	  9822,
	 10360,
	 10111,
	 10379,
	 10379,
	 10379,
	  9831,
	 10285,
	 10060,
	 10211,
	 10061,
	 10263,
	  9782,
	  9788,
	 10327,
	 10152,
	 10361,
	  9780,
	  9982,
	 10284,
	 10307,
	 10318,
	 10375,
	  9783,
	  9905,
	 10130,
	  9960,
	  9782,
	  9796,
	 10113,
	  9798,
	  9854,
	  9918,
	  9784,
	 10379,
	 10379,
	 10199,
	 10379,
	 10379,
	 10379,
	 10379,
	 10379,
	  9981,
	  9782,
	  9781,
	 10300,
	  9792,
	  9782,
	  9782,
	  9862,
	  9782,
	  9782};

	// public static int[] inBlockNum = new int[BLOCKNUM];
	// public static void setInBlockNum() throws IOException {
	// 	String fileName = "blocks.txt";
	// 	FileReader f = new FileReader(fileName);
	// 	BufferedReader br = new BufferedReader(f);
	// 	for (int i = 0; i < BLOCKNUM; i ++) {
	// 		String s = br.readLine();
	// 		inBlockNum[i] = Integer.parseInt(s.trim());
	// 	}
	// }

	// public static final int NUM_ALL_NODE = 10;
	// public static int[] inBlockNum = new int[] {4, 3, 3};
}