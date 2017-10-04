import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Counter;

import java.util.HashMap;
import java.io.*;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Iterator;
import java.util.LinkedList;

//Reducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT>
public class BlockReducerJacobi extends Reducer<Text, Text, Text, Text> {

	private double d = 0.85;

	@Override
	public void reduce (Text key, Iterable<Text> values, Context context) 
		throws IOException, InterruptedException 
	{
	  	// input1: key: <block id>, 
	 	// 			value: "#" + " " + <url> + " " + <block id (in)> + " "+ <url (in)> + " "  + <curRank / degree (in)>
	 	// input2: key: <block id>, 
	 	// 			value: "@" + " " + <url> + “ ” + <curRank> + " " + <degree> + " " + <list of {<block id (out)> + " " +  <urlOut>}>
		
		String bid = key.toString();
		HashMap<String, Node> nodeMap = new HashMap<String, Node>();
		int numOfNode = Constant.inBlockNum[Integer.parseInt(bid)];
		double resChangeInBlock;
		double resChangeGlobal;

		// input2: whole information of the nodes in block
		//			key: <block id>, 
	 	// 			value: "@" + " " + <url> + “ ” + <curRank> + " " + <degree> + " " + <list of {<block id (out)> + " " +  <urlOut>}>
		for (Text value: values) {
			String tmp = value.toString();
			Node node;

			// input1: edge information (including edge within block and edge come from out block)
			//			key: <block id>, 
	 		// 			value: "#" + " " + <url> + " " + <block id (in)> + " "+ <url (in)> + " "  + <curRank / degree (in)>
	 		if ('#' == tmp.charAt(0)) {
				String[] tmpParts = tmp.split("\\s+");
				String url = tmpParts[1].trim();
				String inBlockId = tmpParts[2].trim();
				String inUrl = tmpParts[3].trim();
				double passRank = Double.parseDouble(tmpParts[4].trim());
	 			if (null == nodeMap.get(url)) {
	 				node = new Node(bid, url, "");
	 			} else {
	 				node = nodeMap.get(url);
	 			}

				// the edges from Nodes in Block(bid)
				if (inBlockId.equals(bid)) {
					node.addInNode(inUrl);
				} 
				// boundary conditions
				else {
					node.addBoundary(passRank);
				}
				nodeMap.put(url, node);
	 		}
			// input2: whole information of the nodes in block
			//			key: <block id>, 
		 	// 			value: "@" + " " + <url> + “ ” + <curRank> + " " + <degree> + " " + <list of {<block id (out)> + " " +  <urlOut
			else if ('@' == tmp.charAt(0)) {
				String tmpValue = tmp.substring(2);
				String url = tmp.split("\\s+")[1].trim();
				if (null == nodeMap.get(url)) {
					node = new Node(bid,tmpValue);
				} else {
					node = nodeMap.get(url);
					node.setNodeInfo(tmpValue);
				}
				nodeMap.put(url, node);
			}
		}

		double[] res = iterateInBlockRPOnce(bid, nodeMap);
		resChangeInBlock = res[0];
		resChangeGlobal = res[1];
		long iter = 1;
		while (Constant.EPSILON < resChangeInBlock / numOfNode) {
			res = iterateInBlockRPOnce(bid, nodeMap);
			resChangeInBlock = res[0];
			resChangeGlobal = res[1];
			iter ++;
		}

		// increase global residuals counter
		Counter residualsCount = context.getCounter(BlockDriver.PRCounters.residuals);
		Counter iterCount = context.getCounter(BlockDriver.PRCounters.numOfIter);
		residualsCount.increment((long)(resChangeGlobal * Constant.DECIMAL_ACCURACY));
		iterCount.increment((long)iter);

		if (Constant.SHOW_PAGERANK) {
			// get the two lowest numbered Nodes in block
			int preNodeNum = 0;
			for (int i = 0; i < Integer.parseInt(bid); i ++) {
				preNodeNum += Constant.inBlockNum[i];
			}
			for (int i = 0; i < 2; i ++) {
				String u = (preNodeNum + i) + "";
				Node node = nodeMap.get(u);
				System.out.println("Block " + node.getBlockId() + ", node " + node.getUrl() + ", PageRank " + node.getCurRank());
			}
		}

		// output <key, value>
		Iterator<String> keyIter = nodeMap.keySet().iterator();
		while(keyIter.hasNext()) {
			Node node = nodeMap.get(keyIter.next());
			context.write(new Text(node.setReduceOutKey()), new Text(node.setReduceOutValue()));
		}
	}

	/**
	 * perform a PageRank iteration inside block(blockId)
	 * @param blockId: block id
	 * @param nodeMapInBlock: 
	 * @return values for resChangeInBlock and resChangeGlobal
	 */
	public double[] iterateInBlockRPOnce(String blockId, HashMap<String, Node> nodeMapInBlock) {
		double resInBlockSum = 0;
		double resForGlobalSum = 0;
		double[] res = new double[2];
		Iterator<String> keyIter = nodeMapInBlock.keySet().iterator();
		while(keyIter.hasNext()) {
			Node node = nodeMapInBlock.get(keyIter.next());
			node.setNewRank(0);
			LinkedList<String> inUrlList = node.getInUrlList();

			// the edges from Nodes in Block(blockId)
			for (int i = 0; i < inUrlList.size(); i ++) {
				String inUrl = inUrlList.get(i);
				double passRank = nodeMapInBlock.get(inUrl).getCurRank() / nodeMapInBlock.get(inUrl).getDegree();
				node.setNewRank(node.getNewRank() + passRank);
			}
			// boundary condition
			node.setNewRank(node.getNewRank() + node.getSumBoundaryPassRank());

			// set new page rank
			node.setNewRank(d * node.getNewRank() + (1 - d) / Constant.NUM_ALL_NODE);
			//System.out.println("block " + blockId + ", " + "node " + node.getUrl() + ", curRank " + node.getCurRank() + ", newRank " + node.getNewRank());

		}

		// Jacobi: update curRank and increment the total inBlock residuals 
		keyIter = nodeMapInBlock.keySet().iterator();
		while(keyIter.hasNext()) {
			Node node = nodeMapInBlock.get(keyIter.next());
			resInBlockSum += Math.abs(node.getCurRank() - node.getNewRank()) / node.getNewRank();
			resForGlobalSum += Math.abs(node.getStartRank() - node.getNewRank()) / node.getNewRank();
			node.setCurRank(node.getNewRank());
		}

		res[0] = resInBlockSum;
		res[1] = resForGlobalSum;
		return res;
	}
}