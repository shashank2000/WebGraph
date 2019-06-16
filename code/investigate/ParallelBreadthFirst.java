import java.io.IOException;
import java.io.File;
import java.io.FileWriter;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.Collections;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Random;

import java.util.concurrent.CyclicBarrier;

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

public class ParallelBreadthFirst {
	/** The graph under examination. */
	public final ImmutableGraph graph;
	/** The number of threads. */
	private final int numberOfThreads;
	/** The barrier used to synchronize visiting threads. */
	private volatile CyclicBarrier barrier;
	/** If true, the current visit is over. */
	private volatile boolean completed;
	/** Number of nodes in graph */
	private final int numNodes;
	/** Keeps track of problems in visiting threads. */
	private volatile Throwable threadThrowable;
	/** Keeps track of our results*/
	private final List<String> results;
	/** Keeps track of nodes to start from */
	public final ConcurrentLinkedQueue<Integer> startNodes;
	/** The global progress logger. */
	private final ProgressLogger pl;

	private ParallelBreadthFirst(final ImmutableGraph graph, final int requestedThreads, final Set starts, final ProgressLogger pl) {
		this.graph = graph;
		this.pl = pl;
		// https://stackoverflow.com/questions/8203864/choosing-the-best-concurrency-list-in-java
		this.results = Collections.synchronizedList(new ArrayList<String>());
		this.startNodes = new ConcurrentLinkedQueue<Integer>();
		this.startNodes.addAll(starts);

		numberOfThreads = requestedThreads != 0 ? requestedThreads : Runtime.getRuntime().availableProcessors();
		numNodes = graph.numNodes();
	}

	private final class IterationThread extends Thread {
		private static final int GRANULARITY = 1000;

		@Override
		public void run() {
			try {
				// We cache frequently used fields.
				final ImmutableGraph graph = ParallelBreadthFirst.this.graph.copy();

				for(;;) {
					barrier.await();
					if (completed) return;

					final int start = startNodes.poll();

					// perform BFS
					final IntArrayFIFOQueue queue = new IntArrayFIFOQueue();
					final int[] dist = new int[numNodes];

					Arrays.fill(dist, Integer.MAX_VALUE); // Initially, all distances are infinity.
					final int lo = start == -1 ? 0 : start;
					final int hi = start == -1 ? numNodes : start + 1;

					int curr = lo, succ, ecc = 0, reachable = 0;

					for(int i = lo; i < hi; i++) {
						if (dist[i] == Integer.MAX_VALUE) { // Not already visited
							queue.enqueue(i);
							dist[i] = 0;

							LazyIntIterator successors;

							while(! queue.isEmpty()) {
								curr = queue.dequeueInt();
								successors = graph.successors(curr);
								int d = graph.outdegree(curr);
								while(d-- != 0) {
									succ = successors.nextInt();
									// && dist[curr] + 1 <= maxDist
									if (dist[succ] == Integer.MAX_VALUE ) {
										reachable++;
										dist[succ] = dist[curr] + 1;
										ecc = Math.max(ecc, dist[succ]);
										queue.enqueue(succ);
									}
								}
							}
						}
					}
					String result = start + "\t" + ecc + "\t" + reachable + "\n";
					
					synchronized(results) {
						results.add(result);
					}
				}
			}
			catch(Throwable t) {
				threadThrowable = t;
			}
		}
	}

	public List<String> bfs(){
		completed = false;

		final IterationThread[] thread = new IterationThread[numberOfThreads];
		for(int i = thread.length; i-- != 0;) thread[i] = new IterationThread();

		if (pl != null) {
			pl.start("Starting visit...");
			pl.expectedUpdates = startNodes.size();
			pl.itemsName = "nodes";
		}

		barrier = new CyclicBarrier(numberOfThreads, new Runnable() {
			@Override
			public void run() {
				if (startNodes.size() == 0) {
					completed = true;
					return;
				}

				if (pl != null) pl.update();
			}
		}
		);

		for(int i = thread.length; i-- != 0;) thread[i].start();
		for(int i = thread.length; i-- != 0;) {
			try {
				thread[i].join();
			}
			catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		}

		if (threadThrowable != null) throw new RuntimeException(threadThrowable);
		if (pl != null) pl.done();

		return results;
	}

	static public void main(String arg[]) throws IllegalArgumentException, SecurityException, IllegalAccessException, InvocationTargetException, NoSuchMethodException, JSAPException, IOException {
		final SimpleJSAP jsap = new SimpleJSAP(ParallelBreadthFirst.class.getName(), "Visits a graph in breadth-first fashion, possibly starting just from a given node.",
				new Parameter[] {
						new FlaggedOption("graphClass", GraphClassParser.getParser(), null, JSAP.NOT_REQUIRED, 'g', "graph-class", "Forces a Java class for the source graph."),
						new FlaggedOption("logInterval", JSAP.LONG_PARSER, Long.toString(ProgressLogger.DEFAULT_LOG_INTERVAL), JSAP.NOT_REQUIRED, 'l', "log-interval", "The minimum time interval between activity logs in milliseconds."),
						new FlaggedOption("numStartNodes", JSAP.INTEGER_PARSER, Integer.toString(100), JSAP.NOT_REQUIRED, 'n', "numStartNodes", "Number of start nodes. Randomly sample nodes apart from nodes passed in via option s (if any)"),
						new FlaggedOption("numThreads", JSAP.INTEGER_PARSER, Integer.toString(0), JSAP.NOT_REQUIRED, 't', "numThreads", "Number of threads to use"),
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
		final int n = graph.numNodes();


		final boolean print = jsapResult.getBoolean("print");
		// We parse the starting node.
		final int numStartNodes = jsapResult.getInt("numStartNodes");
		final int numThreads = jsapResult.getInt("numThreads");
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

		// add in random nodes
		Random rand = new Random();
		rand.setSeed(seed);
		while (starts.size() < numStartNodes) {
			int randInt = rand.nextInt(n);
			starts.add(randInt);
		}

		System.out.println("We will start BFS from " + numStartNodes + " nodes");
		System.out.println(Arrays.toString(starts.toArray()));


		// perform bfs
		ParallelBreadthFirst pbf = new ParallelBreadthFirst(graph, numThreads, starts, pl);
		List<String> bfsResults = pbf.bfs();

		// write result

		System.out.println("Saving results to " + resultsname);

		File f = new File(resultsname);
		if (!f.getParentFile().exists())
		    f.getParentFile().mkdirs();
		if (!f.exists())
    		f.createNewFile();

		FileWriter writer = new FileWriter(resultsname); 
		for(String str: bfsResults) {
		  writer.write(str);
		}
		writer.close();
	}
}
