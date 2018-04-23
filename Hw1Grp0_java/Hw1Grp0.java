
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
	/**
	 * put data into table eg.put tableName, key, "res:R4.x", value
	 */
	@SuppressWarnings("deprecation")
	private void writeToTable(HTable table, String key, String prefix, int index,
			ArrayList<Integer> resColumn, String line) throws IOException {
		String[] cols = line.split("\\|");
		for (int i = 0; i < resColumn.size(); ++i) {
			String columnName = prefix + resColumn.get(i);
			if (index != 0) {
				columnName += "." + index;
			}

			Put put = new Put(key.getBytes());
			put.add("res".getBytes(), columnName.getBytes(), cols[resColumn.get(i)].getBytes());
			table.put(put);
			System.out.format("# put table=%s, key=%s, res:%s, %s\n", "Result", key, columnName, cols[resColumn.get(i)]);
		}
	}
	
	/**
	 * function readHDFS
	 *
	 * @param filePath
	 * @return BufferedReader
	 *
	 */
	private BufferedReader readHDFS(String filePath) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(filePath), conf);
		Path path = new Path(filePath);
		FSDataInputStream in_stream = fs.open(path);
		
		return new BufferedReader(new InputStreamReader(in_stream));
	}

	/**
	 * function hashJoin
	 *
	 * @param pathR
	 * @param pathS
	 * @param joinR
	 * @param joinS
	 * @param resColumnR
	 * @param resColumnS
	 *
	 */
	private void hashJoin(String pathR, String pathS, int joinR, int joinS, ArrayList<Integer> resColumnR, ArrayList<Integer> resColumnS) throws IOException {
		Logger.getRootLogger().setLevel(Level.WARN);
		Configuration configuration = HBaseConfiguration.create();
		@SuppressWarnings("deprecation")
		HBaseAdmin hAdmin = new HBaseAdmin(configuration);

		String tableName = "Result";
		HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));

		HColumnDescriptor cf = new HColumnDescriptor("res");
		htd.addFamily(cf);

		if (hAdmin.tableExists(tableName)) {
			System.out.println("Table already exists. Deleting...");
			hAdmin.disableTable(tableName);
			hAdmin.deleteTable(tableName);
		}
		hAdmin.createTable(htd);
		System.out.println("Table " + tableName + " created successfully.");
		hAdmin.close();
		@SuppressWarnings("deprecation")
		HTable table = new HTable(configuration, tableName);
		
		// Get input
		BufferedReader inR = readHDFS(pathR);
        BufferedReader inS = readHDFS(pathS);
        
        // Hash R
        Hashtable<String, ArrayList<String>> hashTable = new Hashtable<>();
		String lineR;
		while ((lineR = inR.readLine()) != null) {
			String[] colsLineR = lineR.split("\\|");
			String joinKeyR = colsLineR[joinR];

			if (!hashTable.containsKey(joinKeyR)) {
				ArrayList<String> arr = new ArrayList<>();
				arr.add(lineR);
				hashTable.put(joinKeyR, arr);
			} else {
				ArrayList<String> arr = hashTable.get(joinKeyR);
				arr.add(lineR);
			}
		}
		
		// Join S
		String lineS;
		while ((lineS = inS.readLine()) != null) {
			String[] colsLineS = lineS.split("\\|");
			String joinKeyS = colsLineS[joinS];

			if (hashTable.containsKey(joinKeyS)) {
				ArrayList<String> linesR = hashTable.get(joinKeyS);
				Iterator<String> it = linesR.iterator();
				int index = 0;
				while (it.hasNext()) {
					String curLineR = it.next();
					System.out.format("\n# LineR: %s\n", curLineR);
					System.out.format("# LineS: %s\n", lineS);
					writeToTable(table, joinKeyS, "R", index, resColumnR, curLineR);
					writeToTable(table, joinKeyS, "S", index, resColumnS, lineS);
					++index;
				}
			}
		}
		
		// Close resources
		inR.close();
		inS.close();
		table.close();
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
		 */
		String fileR = "hdfs://localhost:9000";
		String fileS = "hdfs://localhost:9000";
		if (args[0].startsWith("R") && args[1].startsWith("S")) {
			fileR += args[0].replace("R=", "");
			fileS += args[1].replace("S=", "");
		} else {
			fileR += args[1].replace("R=", "");
			fileS += args[0].replace("S=", "");
		}
		
		/**
		 * Get join columns
		 * support join:R2=S3 or join:S2=R3 etc.
		 */
		String[] joins = args[2].replace("join:", "").split("=");
		int joinColumnR = 0;
		int joinColumnS = 0;
		if (joins.length == 2) {
			if (joins[0].startsWith("R") && joins[1].startsWith("S")) {
				joinColumnR = Integer.parseInt(joins[0].replace("R", ""));
				joinColumnS = Integer.parseInt(joins[1].replace("S", ""));
			} else {
				joinColumnR = Integer.parseInt(joins[1].replace("R", ""));
				joinColumnS = Integer.parseInt(joins[0].replace("S", ""));
			}
		}

		/**
		 * Get res columns
		 * support res non-ordered input: S1,R1,S2 or R2,S3,R4 or S1,R2,R5,S3 etc.
		 */
		String[] res = args[3].replace("res:", "").split(",");
		ArrayList<Integer> resColumnR = new ArrayList<>();
		ArrayList<Integer> resColumnS = new ArrayList<>();
		for (int i = 0; i < res.length; ++i) {
			if (res[i].startsWith("R")) {
				resColumnR.add(Integer.parseInt(res[i].replaceAll("[^0-9]", "")));
			} 
			if (res[i].startsWith("S")) {
				resColumnS.add(Integer.parseInt(res[i].replaceAll("[^0-9]", "")));
			}
		}
		
		/**
		 * Excute
		 */
		Hw1Grp0 work = new Hw1Grp0();
		work.hashJoin(fileR, fileS, joinColumnR, joinColumnS, resColumnR, resColumnS);
	}
}
