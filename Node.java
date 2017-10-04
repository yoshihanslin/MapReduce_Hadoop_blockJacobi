import java.util.LinkedList;

/** 
 * Node helper: includes mapper and reducer
 * 
 * Mapper 
 *	input: <block id> + " " + <url> + “ ” + <curRank> + " " + <degree> + " " + <list of {<block id (out)> + " " +  <urlOut>}>
 * 	output1: key: <block id (out)>, 
 *			value: "#" + " " + <urlOut> + " " + <block id> + " " + <url> + " " + <curRank / degree>
 *	output2: key: <block id>, 
 * 			value: "@" + " " + <url> + “ ” + <curRank> + " " + <degree> + " " + <list of {<block id (out)> + " " +  <urlOut>}>
 * 
 * Reducer 
 * 	input1: key: <block id>, 
 *			value: "#" + " " + <url> + " " + <block id (in)> + " "+ <url (in)> + " "  + <curRank / degree (in)>
 *	input2: key: <block id>, 
 * 			value: "@" + " " + <url> + “ ” + <curRank> + " " + <degree> + " " + <list of {<block id (out)> + " " +  <urlOut>}>
 *	output: key: <block id> + " " + <url>, 
 *			value: <newRank> + " " + <degree> + " " + <list of <block id (out), urlOut>>
 * 
 * @author jingyi
 *
 */
public class Node {
	
	private String blockId;		// block id
	private String url;			// node id
	private double curRank;		// current page rank
	private int degree;			// out degree
	private double newRank;		// new page rank
	private double startRank;	// start page rank, the page rank at the beginning of inblock iteration
	private double sumBoundaryPassRank;		// sum of boundary incoming passRank (PageRank / degree)
	private LinkedList<String> inUrlList;	// list of source nodes of incoming edge
	private LinkedList<OutNode> outList;	// list of destination nodes of outgoing edge
	
	/**
	 * constructor for mapper input: line (don't need to use startRank)
	 * @param line: <block id> + " " + <url> + “ ” + <curRank> + " " + <degree> + " " + <list of {<block id (out)> + " " +  <urlOut>}>
	 */
	public Node(String line) {
		String[] parts = line.split("\\s+");
		this.blockId = parts[0].trim();
		this.url = parts[1].trim();
		this.curRank = Double.parseDouble(parts[2].trim());
		this.degree = Integer.parseInt(parts[3].trim());
		this.sumBoundaryPassRank = 0;
		this.outList = new LinkedList<OutNode>();
		for (int i = 0; i < degree; i++) {
			OutNode out = new OutNode(parts[4 + 2 * i], parts[4 + 2 * i + 1]);
			this.outList.add(out);
		}
		this.inUrlList = new LinkedList<String>();
	}

	/**
	 * constructor for reduce input2 "@"
	 * 		key: <block id>,
	 * 		value: "@" + " " + <url> + “ ” + <curRank> + " " + <degree> + " " + <list of {<block id (out)> + " " +  <urlOut>}>
	 * @param key: <block id>
	 * @param tmpValue: <url> + “ ” + <curRank> + " " + <degree> + " " + <list of {<block id (out)> + " " +  <urlOut>}>
	 */
	public Node(String key, String tmpValue) {
		this.blockId = key;
		String[] parts = tmpValue.split("\\s+");
		this.url = parts[0].trim();
		this.curRank = Double.parseDouble(parts[1].trim());
		this.startRank = this.curRank;
		this.degree = Integer.parseInt(parts[2].trim());
		this.sumBoundaryPassRank = 0;
		this.inUrlList = new LinkedList<String>();
		this.outList = new LinkedList<OutNode>();
		for (int i = 0; i < degree; i++) {
			OutNode out = new OutNode(parts[3 + 2 * i], parts[3 + 2 * i + 1]);
			this.outList.add(out);
		}
	}
	
	/**
	 * constructor for reduce input1 "#"
	 * 		key: <block id>, 
	 * 		value: "#" + " " + <url> + " " + <block id (in)> + " "+ <url (in)> + " "  + <curRank / degree (in)>
	 * @param key: <block id>
	 * @param url: <url>
	 * @param dummy: ""
	 */
	public Node(String key, String url, String dummy) {
		this.blockId = key;
		this.url = url;
		this.sumBoundaryPassRank = 0;
		this.inUrlList = new LinkedList<String>();
	}
	
	/**
	 * update Node information for reduce input2 "@" after construct Node
	 * @param tmpValue <url> + “ ” + <curRank> + " " + <degree> + " " + <list of {<block id (out)> + " " +  <urlOut>}>
	 */
	public void setNodeInfo(String tmpValue) {
		String[] parts = tmpValue.split("\\s+");
		this.url = parts[0].trim();
		this.curRank = Double.parseDouble(parts[1].trim());
		this.startRank = this.curRank;
		this.degree = Integer.parseInt(parts[2].trim());
		this.outList = new LinkedList<OutNode>();
		for (int i = 0; i < degree; i++) {
			OutNode out = new OutNode(parts[3 + 2 * i], parts[3 + 2 * i + 1]);
			this.outList.add(out);
		}
	}

	/**
	 * Mapper output1: key: <block id (out)>, 
	 *				value: "#" + " " + <urlOut> + " " + <block id> + " " + <url> + " " + <curRank / degree>
	 * @param urlOut
	 * @return output value1 of Mapper
	 */
	public String setMapOutValue1 (String out) {
		return "#" + " " + out + " " + this.blockId + " " + this.url + " "+ this.curRank / this.degree; 
	}

	/**
	* Mapper output2: key: <block id>, 
	* 				value: "@" + " " + <url> + “ ” + <curRank> + " " + <degree> + " " + <list of <block id (out), urlOut>>
	* @param one line from input file
	* @return output value2 of Mapper
	*/
	public static String setMapOutValue2 (String line) {
		String[] parts = line.split("\\s+");
		return "@" + " " + line.substring(parts[0].length() + 1);
	}

	/**
	* Reducer output: key: <block id> + " " + <url>, 
 	*				value: <newRank> + " " + <degree> + " " + <list of <block id (out), urlOut>>
	* @return reduce output key
	*/
	public String setReduceOutKey() {
		return blockId + " " + url;
	}

	/**
	* Reducer output: key: <block id> + " " + <url>, 
 	*				value: <newRank> + " " + <degree> + " " + <list of <block id (out), urlOut>>
	* @return reduce output value
	*/
	public String setReduceOutValue() {
		String value;
		value = newRank + " " + degree;
		for (OutNode out: outList) {
			value += (" " + out.getBlockId() + " " + out.getUrl());
		}
		return value;
	}
	
	/**
	 * add source node of the incoming edge to inUrlList
	 * @param url: node id of incoming node
	 */
	public void addInNode(String url) {
		this.inUrlList.add(url);
	}
	
	/**
	 * increase the sum of boundary pagerank / degree of source nodes from other block 
	 * @param passRank: pagerank / degree of nodes from other block
	 */
	public void addBoundary(double passRank) {
		this.sumBoundaryPassRank = this.sumBoundaryPassRank + passRank;
	}
	
	/**
	 * information of destination node
	 * @author jingyi
	 *
	 */
	public class OutNode {
		private String blockId;		// block id of destination node
		private String url;			// node id of destination node
		public OutNode(String bid, String u) {
			blockId = bid;
			url = u;
		}
		public String getBlockId() {
			return blockId;
		}
		public String getUrl() {
			return url;
		}
	}

	public String getBlockId() {
		return blockId;
	}

	public void setBlockId(String blockId) {
		this.blockId = blockId;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public double getCurRank() {
		return curRank;
	}

	public void setCurRank(double curRank) {
		this.curRank = curRank;
	}

	public double getDegree() {
		return degree;
	}

	public void setDegree(int degree) {
		this.degree = degree;
	}

	public double getNewRank() {
		return newRank;
	}

	public void setNewRank(double newRank) {
		this.newRank = newRank;
	}

	public double getStartRank() {
		return startRank;
	}

	public void setStartRank(double startRank) {
		this.startRank = startRank;
	}

	public double getSumBoundaryPassRank() {
		return sumBoundaryPassRank;
	}

	public void setSumBoundaryPassRank(double sumBoundaryPassRank) {
		this.sumBoundaryPassRank = sumBoundaryPassRank;
	}

	public LinkedList<OutNode> getOutList() {
		return outList;
	}

	public void setOutList(LinkedList<OutNode> outList) {
		this.outList = outList;
	}

	public LinkedList<String> getInUrlList() {
		return inUrlList;
	}

	public void setInUrlList(LinkedList<String> inUrlList) {
		this.inUrlList = inUrlList;
	}

}