import java.io.IOException;
import it.unimi.dsi.fastutil.io.BinIO;

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

public class NodeNumberToHex {
	/** The extension of the identifier file (a binary list of longs). */
	private static final String IDS_EXTENSION = ".ids";

	private NodeNumberToHex(){}

	static public void main(String arg[]) throws IllegalArgumentException, JSAPException, IOException {
		final SimpleJSAP jsap = new SimpleJSAP(NodeNumberToHex.class.getName(), "Visits a graph in breadth-first fashion, possibly starting just from a given node.",
				new Parameter[] {
						new FlaggedOption("node", JSAP.INTEGER_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'n', "node", "node to get the original value"),
						new UnflaggedOption("basename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the graph."),
					}
				);

		final JSAPResult jsapResult = jsap.parse(arg);
		if (jsap.messagePrinted()) System.exit(1);

		final String basename = jsapResult.getString("basename");
		long[][] ids = BinIO.loadLongsBig(basename + IDS_EXTENSION);

		final int node = jsapResult.getInt("node");

		if (node > ids[0].length){
			throw new IllegalArgumentException("Out of bounds");
		}

		long id = ids[0][node];
		String hexVal = Long.toHexString(id);
		String paddedVal = "0000000000000000".substring(hexVal.length()) + hexVal;


		System.out.println("Node " + node + " in webgraph is originally long : " + id + " hex : " + paddedVal);
	}
}
