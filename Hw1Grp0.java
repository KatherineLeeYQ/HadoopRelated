
/**
 * Make sure that the classpath contains all the hbase libraries
 *
 * Compile:
 *  javac Hw1Grp0.java
 *
 * Run:
 *  java Hw1Grp0
 */
import java.io.IOException;
import java.io.*;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.ArrayList;

import java.net.URI;
import java.net.URISyntaxException;

import org.apache.log4j.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Class Hw1Grp0
 * 
 * @author Katherine
 *
 */
public class Hw1Grp0 {
	static String getOperatedValue(ArrayList<Integer> columns, String[] resCols) {
		String value = "";

		Iterator<Integer> it = columns.iterator();
		while (it.hasNext()) {
			value += resCols[it.next()] + ",";
		}

		return value;
	}

	@SuppressWarnings("deprecation")
	static void writeToTable(HTable table, String key, String prefix, int index,
			ArrayList<Integer> resColumn, String res) throws IOException {
		/**
		 * put data into table eg.put tableName, key, "res:R4.x", value
		 */
		String[] cols = res.split(",");
		for (int i = 0; i < cols.length; ++i) {
			String columnName = prefix + resColumn.get(i);
			if (index != 0) {
				columnName += "." + index;
			}

			Put put = new Put(key.getBytes());
			put.add("res".getBytes(), columnName.getBytes(), cols[i].getBytes());
			table.put(put);

			//System.out.format("put %s, %s, res:%s, %s\n", "Result", key, columnName, cols[i]);
		}
	}

	/**
	 * function main
	 *
	 * @param args
	 *            arguments
	 * @return void
	 *
	 */
	public static void main(String[] args)
			throws IOException, URISyntaxException, MasterNotRunningException, ZooKeeperConnectionException {
		/**
		 * Check args number
		 * if the length of args less than 4
		 * then return
		 */
		if (args.length < 4) {
			String hint1 = "Please enter args like this format:\n";
			String hint2 = "java Hw1Grp0 R=/hw1/1.txt S=/hw1/2.txt join:R2=S3 res:R4,S5";
			System.out.println(hint1 + hint2);
			return;
		}
		
		/**
		 * Get file paths
		 * support R=/hw1/1.txt S=/hw1/2.txt or S=/hw1/1.txt R=/hw1/2.txt
		 * filePath1 place for R file path
		 * filePath2 place for S file path
		 */
		String filePath1 = args[0].substring(3); // R
		String filePath2 = args[1].substring(3); // S
		if (args[0].charAt(0) == 'S') {
			String tmp = filePath1;
			filePath1 = filePath2;
			filePath2 = tmp;
		}
		
		/**
		 * Get join columns
		 * support join:R2=S3 or join:S2=R3 etc.
		 * joinColumn1 place for R column number
		 * joinColumn2 place for S column number
		 */
		int joinColumn1 = Integer.parseInt(args[2].substring(6, 7));	// R
		int joinColumn2 = Integer.parseInt(args[2].substring(9, 10));	// S
		if (args[2].charAt(5) == 'S') {
			int tmp = joinColumn1;
			joinColumn1 = joinColumn2;
			joinColumn2 = tmp;
		}

		/**
		 * Get res columns
		 * support res non-ordered input: S1,R1,S2 or R2,S3,R4 or S1,R2,R5,S3 etc.
		 */
		String[] res = args[3].substring(4).split(",");
		ArrayList<Integer> resColumn1 = new ArrayList<>();	// R
		ArrayList<Integer> resColumn2 = new ArrayList<>();	// S
		for (int i = 0; i < res.length; ++i) {
			if (res[i].charAt(0) == 'R') {
				resColumn1.add(Integer.parseInt(res[i].substring(1)));
			} else {
				resColumn2.add(Integer.parseInt(res[i].substring(1)));
			}
		}

		/**
		 * HDFS Operation
		 */
		String file1 = "hdfs://localhost:9000/" + filePath1;
		String file2 = "hdfs://localhost:9000/" + filePath2;

		Configuration conf = new Configuration();

		FileSystem fs1 = FileSystem.get(URI.create(file1), conf);
		Path path1 = new Path(file1);
		FSDataInputStream in_stream1 = fs1.open(path1);
		BufferedReader in1 = new BufferedReader(new InputStreamReader(in_stream1));

		FileSystem fs2 = FileSystem.get(URI.create(file1), conf);
		Path path2 = new Path(file2);
		FSDataInputStream in_stream2 = fs1.open(path2);
		BufferedReader in2 = new BufferedReader(new InputStreamReader(in_stream2));

		/**
		 * HBase Operation
		 */
		Logger.getRootLogger().setLevel(Level.WARN);
		Configuration configuration = HBaseConfiguration.create();
		@SuppressWarnings("deprecation")
		HBaseAdmin hAdmin = new HBaseAdmin(configuration);

		/**
		 * Create table descriptor
		 */
		String tableName = "Result";
		HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));

		/**
		 * Create column descriptor
		 */
		HColumnDescriptor cf = new HColumnDescriptor("res");
		htd.addFamily(cf);

		/**
		 * Create table
		 */
		if (hAdmin.tableExists(tableName)) {
			//System.out.println("Table already exists. Deleting...");
			hAdmin.disableTable(tableName);
			hAdmin.deleteTable(tableName);
		}
		hAdmin.createTable(htd);
		//System.out.println("Table " + tableName + " created successfully.");
		hAdmin.close();
		@SuppressWarnings("deprecation")
		HTable table = new HTable(configuration, tableName);

		/**
		 * Hash R
		 */
		Hashtable<String, ArrayList<String>> hashTable = new Hashtable<>();
		String s1;
		while ((s1 = in1.readLine()) != null) {
			String[] cols = s1.split("\\|");
			String joinKey1 = cols[joinColumn1];
			String value1 = getOperatedValue(resColumn1, cols);

			/**
			 * put the operated value into list
			 */
			if (!hashTable.containsKey(joinKey1)) {
				ArrayList<String> arr = new ArrayList<>();
				arr.add(value1);
				hashTable.put(joinKey1, arr);
			} else {
				ArrayList<String> arr = hashTable.get(joinKey1);
				arr.add(value1);
			}
		}

		/**
		 * Join S
		 */
		String s2;
		while ((s2 = in2.readLine()) != null) {
			String[] cols = s2.split("\\|");
			String joinKey2 = cols[joinColumn2];
			String value2 = getOperatedValue(resColumn2, cols);

			/**
			 * if key exists, then join
			 */
			if (hashTable.containsKey(joinKey2)) {
				ArrayList<String> valueList = hashTable.get(joinKey2);
				Iterator<String> it = valueList.iterator();
				int index = 0;
				while (it.hasNext()) {
					String value1 = it.next();
					//System.out.format("\n###Key: %s Record %d\n", joinKey2, count++);
					//System.out.format("Resource string: %s\n", value1);
					//System.out.format("Resource string: %s\n", value2);
					writeToTable(table, joinKey2, "R", index, resColumn1, value1);
					writeToTable(table, joinKey2, "S", index, resColumn2, value2);
					++index;
				}
			}
		}

		/**
		 * Close resources
		 */
		in1.close();
		in2.close();
		fs1.close();
		fs2.close();
		table.close();
	}
}
