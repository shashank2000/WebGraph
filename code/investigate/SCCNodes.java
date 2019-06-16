
import it.unimi.dsi.bits.Fast;
import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.io.BinIO;

import java.util.HashSet;
import java.util.Scanner;
import java.util.Random;
import java.util.*;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.Switch;
import com.martiansoftware.jsap.UnflaggedOption;

public class SCCNodes {

	static public void main(String arg[]) throws IllegalArgumentException, SecurityException, JSAPException, IOException, ClassNotFoundException {
		SimpleJSAP jsap = new SimpleJSAP(SCCNodes.class.getName(), "Get some nodes in SCC",
				new Parameter[] {		
						new FlaggedOption("numNodes", JSAP.INTEGER_PARSER, Integer.toString(10), JSAP.NOT_REQUIRED, 'n', "numNodes", "Number of nodes to retrieve that are in SCC and that are definitely not in SCC"),
						new FlaggedOption("seed", JSAP.INTEGER_PARSER, Integer.toString(0), JSAP.NOT_REQUIRED, 's', "seed", "seed"),
						new UnflaggedOption("basename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the graph."),
						new UnflaggedOption("resultsName", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, JSAP.NOT_GREEDY, "The basename of the resulting files."),
					}
				);

		JSAPResult jsapResult = jsap.parse(arg);
		if (jsap.messagePrinted()) System.exit(1);

		final String basename = jsapResult.getString("basename");
		final int numNodes = jsapResult.getInt("numNodes");
		final int seed = jsapResult.getInt("seed"); 


		final int[] scc = new File(basename + ".scc").exists() ? BinIO.loadInts(basename + ".scc") : null;
		final int[] sccsize = new File(basename + ".sccsizes").exists() ? BinIO.loadInts(basename + ".sccsizes") : null;

		if (scc == null || sccsize == null){
			throw new IllegalArgumentException("No scc computed for given basename " + basename);
		}

		// Get maximum SCC size and index
		int maxSccSize = 0;
		int maxSccInd = 0;
		for (int i = 0; i < sccsize.length; i++) {
			if (sccsize[i] > maxSccSize) {
				maxSccSize = sccsize[i];
				maxSccInd = i;
			}
		}

		System.out.println("Maximum SCC is " + maxSccInd + " with size " + maxSccSize);

		if (numNodes > maxSccSize) {
			throw new IllegalArgumentException("We do not have as many nodes in SCC");
		}

		Set<Integer> inMaxSCCNodes = new HashSet<Integer>();
		Set<Integer> notInMaxSCCNodes = new HashSet<Integer>();

		int n = scc.length;

		// add in random nodes
		Random rand = new Random();
		rand.setSeed(seed);
	
		while (inMaxSCCNodes.size() < numNodes || notInMaxSCCNodes.size() < numNodes) {
			int i = rand.nextInt(n);
			if (scc[i] == maxSccInd & inMaxSCCNodes.size() < numNodes) {
				inMaxSCCNodes.add(i);
			} else if (scc[i] != maxSccInd & notInMaxSCCNodes.size() < numNodes) {
				notInMaxSCCNodes.add(i);
			}
		}

		System.out.println("Nodes that are in Biggest SCC");
		System.out.println(Arrays.toString(inMaxSCCNodes.toArray()));

		System.out.println("Nodes that are not in Biggest SCC");
		System.out.println(Arrays.toString(notInMaxSCCNodes.toArray()));

		if (jsapResult.userSpecified("resultsName")) {
			final String resultsname = jsapResult.getString("resultsName");			

			System.out.println("Saving results to " + resultsname);

			File f = new File(resultsname);
			if (!f.getParentFile().exists())
			    f.getParentFile().mkdirs();
			if (!f.exists())
	    		f.createNewFile();

			FileWriter writer = new FileWriter(resultsname); 
			for (Integer node: inMaxSCCNodes) {
			  writer.write(node + "\n");
			}

			writer.write("\n");

			for (Integer node: notInMaxSCCNodes) {
			  writer.write(node + "\n");
			}
			writer.close();

		}
	}
}
