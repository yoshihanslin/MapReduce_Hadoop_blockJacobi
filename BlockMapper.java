import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//Mapper<KEYIN,VALUEIN,KEYOUT,VALUEOUT>
public class BlockMapper extends Mapper<Object, Text, Text, Text> {

 	@Override
 	public void map (Object key, Text value, Context context) 
 		throws IOException, InterruptedException 
 	{
		// original round: input from input file
		// line: <block id> + " " + <url> + “ ” + <curRank> + " " + <degree> + " " + <list of {<block id (out)> + " " +  <urlOut>}>
 		// other found: input from output file of reducer
 		// line: <block id> + " " + <url> + “/t” + <curRank> + " " + <degree> + " " + <list of {<block id (out)> + " " +  <urlOut>}>

		String line = value.toString();
		Node node = new Node(line);

		// output1: key: <block id (out)>, 
 		//			value: "#" + " " + <urlOut> + " " + <block id> + " " + <url> + " " + <curRank / degree>
 		for (Node.OutNode out: node.getOutList()) {  // out: <block id (out), urlOut>
 			String outValue1 = node.setMapOutValue1(out.getUrl());
 			context.write(new Text(out.getBlockId()), new Text(outValue1));
 		}

	 	//output2: key: <block id>, 
	  	//			value: "@" + " " + <url> + “ ” + <curRank> + " " + <degree> + " " + <list of {<block id (out)> + " " +  <urlOut>}>
		String outValue2 = Node.setMapOutValue2(line);
		context.write(new Text(node.getBlockId()), new Text(outValue2));
 	}
 }