import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.io.File;
import java.io.FileWriter;
import java.util.*;
import java.util.Collections;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Random;

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.Switch;
import com.martiansoftware.jsap.UnflaggedOption;

/*
 * Copyright (C) 2003-2017 Paolo Boldi and Sebastiano Vigna
 *
 *  This program is free software; you can redistribute it and/or modify it
 *  under the terms of the GNU General Public License as published by the Free
 *  Software Foundation; either version 3 of the License, or (at your option)
 *  any later version.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 *  or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 *  for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses/>.
 *
 */


import it.unimi.dsi.fastutil.ints.IntArrayFIFOQueue;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.webgraph.GraphClassParser;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.LazyIntIterator;

/** The main method of this class loads an arbitrary {@link it.unimi.dsi.webgraph.ImmutableGraph}
 * and performs a breadth-first visit of the graph (optionally starting just from a given node, if provided,
 * in which case it prints the eccentricity of the node, i.e., the maximum distance from the node).
 */

public class SequentialBreadthFirst {

	private SequentialBreadthFirst() {}

	static public void main(String arg[]) throws IllegalArgumentException, SecurityException, IllegalAccessException, InvocationTargetException, NoSuchMethodException, JSAPException, IOException {
		final SimpleJSAP jsap = new SimpleJSAP(SequentialBreadthFirst.class.getName(), "Visits a graph in breadth-first fashion, possibly starting just from a given node.",
				new Parameter[] {
						new FlaggedOption("graphClass", GraphClassParser.getParser(), null, JSAP.NOT_REQUIRED, 'g', "graph-class", "Forces a Java class for the source graph."),
						new FlaggedOption("logInterval", JSAP.LONG_PARSER, Long.toString(ProgressLogger.DEFAULT_LOG_INTERVAL), JSAP.NOT_REQUIRED, 'l', "log-interval", "The minimum time interval between activity logs in milliseconds."),
						new FlaggedOption("numStartNodes", JSAP.INTEGER_PARSER, Integer.toString(100), JSAP.NOT_REQUIRED, 'n', "numStartNodes", "Number of start nodes. Randomly sample nodes apart from nodes passed in via option s (if any)"),
						new FlaggedOption("seed", JSAP.INTEGER_PARSER, Integer.toString(0), JSAP.NOT_REQUIRED, 's', "seed", "seed"),
						new Switch("print", 'p', "print", "Print nodes as they are enqueued. If set, ordinary output is suppressed."),
						new UnflaggedOption("basename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the graph."),
						new UnflaggedOption("resultsname", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the resulting file."),
						new UnflaggedOption("startFilename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, JSAP.NOT_GREEDY, "Start nodes of the graph."),
					}
				);

		final JSAPResult jsapResult = jsap.parse(arg);
		if (jsap.messagePrinted()) System.exit(1);

		final ProgressLogger pl = new ProgressLogger();
		pl.logInterval = jsapResult.getLong("logInterval");
		final String basename = jsapResult.getString("basename");
		final ImmutableGraph graph;
		if (jsapResult.userSpecified("graphClass")) graph = (ImmutableGraph)(jsapResult.getClass("graphClass")).getMethod("load", CharSequence.class, ProgressLogger.class).invoke(null, basename, pl);
		else graph = ImmutableGraph.load(basename, pl);

		final boolean print = jsapResult.getBoolean("print");
		final int n = graph.numNodes();

		// We parse the starting node.
		final int numStartNodes = jsapResult.getInt("numStartNodes");
		final String resultsname = jsapResult.getString("resultsname");
		final int seed = jsapResult.getInt("seed"); 


		Set<Integer> starts = new HashSet<Integer>();

		// read in customly designed nodes
		if (jsapResult.userSpecified("startFilename")) {
			final String startFile = jsapResult.getString("startFilename");
			Scanner inFile = new Scanner(new File(startFile));

			while (inFile.hasNext()){
				int startNode = inFile.nextInt();
				starts.add(startNode);
			}
		}

		// add in random nodes if needed
		Random rand = new Random();
		rand.setSeed(seed);
		while (starts.size() < numStartNodes) {
			int randInt = rand.nextInt(n);
			starts.add(randInt);
		}

		System.out.println("We will start BFS from " + numStartNodes + " nodes");
		//System.out.println(Arrays.toString(starts.toArray()));

		ArrayList<Integer> startList = new ArrayList<Integer>();
		startList.addAll(starts);

		if (pl != null) {
			pl.start("Starting visit...");
			pl.expectedUpdates = startList.size();
			pl.itemsName = "nodes";
		}

		ArrayList<String> results = new ArrayList<String>();


		// Perform BFS sequentially
		for (Integer start: startList) {
			// We parse the starting node.
			final IntArrayFIFOQueue queue = new IntArrayFIFOQueue();
			final int[] dist = new int[n];

			Arrays.fill(dist, Integer.MAX_VALUE); // Initially, all distances are infinity.
			final int lo = start == -1 ? 0 : start;
			final int hi = start == -1 ? n : start + 1;

			int curr = lo, succ, ecc = 0, reachable = 0;

			for(int i = lo; i < hi; i++) {
				if (dist[i] == Integer.MAX_VALUE) { // Not already visited
					queue.enqueue(i);
					if (print) System.out.println(i);
					dist[i] = 0;

					LazyIntIterator successors;

					while(! queue.isEmpty()) {
						curr = queue.dequeueInt();
						successors = graph.successors(curr);
						int d = graph.outdegree(curr);
						while(d-- != 0) {
							succ = successors.nextInt();
							if (dist[succ] == Integer.MAX_VALUE) {
								reachable++;
								dist[succ] = dist[curr] + 1;
								ecc = Math.max(ecc, dist[succ]);
								queue.enqueue(succ);
								if (print) System.out.println(succ);
							}
						}
					}
				}	
			}

			System.out.println("The eccentricity of node " + start + " is " + ecc + " (" + reachable + " reachable nodes)");
			String result = start + "\t" + ecc + "\t" + reachable + "\n";
			results.add(result);
			pl.update();
		}

		System.out.println("Saving results to " + resultsname);

		File f = new File(resultsname);
		if (!f.getParentFile().exists())
		    f.getParentFile().mkdirs();
		if (!f.exists())
    		f.createNewFile();

		FileWriter writer = new FileWriter(resultsname); 
		for(String str: results) {
		  	writer.write(str);
		}
		writer.close();
	}
}
