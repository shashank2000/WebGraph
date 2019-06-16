#!/bin/bash


# This file computes bfs for given year, for given graph style
# To run it on regular graphs, set prefix as ""
# To run it on domain graphs, set prefix as "domain_"
# Other parameters include:
# - numSeedPoints: determines how many seed points to use from SCCNodes
#                note, this is for each group (i.e. if 10, we have 10 points from SCC and 10 from non-SCC)
# - includeStats: adds points of interest from stats file in our bfs search
# - numBFSPoints: determins how many points to kick off bfs from
# - runRev: whether to run bfs for reverse graph or not
#
# Note there is a seed fixed, that subsequent runs (should) return same result

# Params
year="$1"
#prefix="domain_"
prefix=""
numSeedPoints=10
includeStats=true
numBFSPoints=50
runRev=true

# environ definition - change as needed
root="/dfs/scratch2/dankang/wb_links/${1}"
base="${root}/${prefix}webgraph"
revbase="${root}/${prefix}webgraph_rev"
codepath="/dfs/scratch2/dankang/WebGraph/code/investigate"
libpath="/dfs/scratch2/dankang/WebGraph/lib/webgraph/*"


cd ${codepath}

classFile=$(find ${codepath}/ -type f -iname "SCCNodes.class")
if [ -z "$classFile" ]; then
    javac SCCNodes.java

    classFile=$(find ${codepath}/ -type f -iname "SCCNodes.class")
    if [ -z "$classFile" ]; then
        echo "[ERROR] Check the path for SCCNodes. Is it in ${codepath}?"
        exit 1
    fi
fi


echo "\n\nGenerating seed points for BFS for ${prefix}graph\n\n"

java -cp "${libpath}:." \
    SCCNodes \
    -n ${numSeedPoints} \
    ${base}/webgraph \
    ${base}/bfs_startnodes

if [ "$includeStats" = true ]; then
    echo "\n\nAdding points from stats to our starting seed points"

    statsFile=$(find ${base}/stats/ -type f -iname "stats.stats")

    if [ -z "$statsFile" ]; then
        echo "StatsFile does not exist!"
        exit 1
    else
        minout=$(cat ${statsFile} | grep minoutdegreenode | cut -d "=" -f 2)
        maxout=$(cat ${statsFile} | grep maxoutdegreenode | cut -d "=" -f 2)
        minin=$(cat ${statsFile} | grep minindegreenode | cut -d "=" -f 2)
        maxin=$(cat ${statsFile} | grep maxindegreenode | cut -d "=" -f 2)

        # append nodes to start of file
        echo "${minout}\n${maxout}\n${minin}\n${maxin}\n\n$(cat ${base}/bfs_startnodes)" > ${base}/bfs_startnodes
    fi
else
    echo "[INFO] Not including stats in our starting seed points"
fi


classFile=$(find ${codepath}/ -type f -iname "SequentialBreadthFirst.class")
if [ -z "$classFile" ]; then
    javac SequentialBreadthFirst.java

    classFile=$(find ${codepath}/ -type f -iname "SequentialBreadthFirst.class")
    if [ -z "$classFile" ]; then
        echo "[ERROR] Check the path for SequentialBreadhFirst. Is it in ${codepath}?"
        exit 1
    fi
fi

echo "\n\nRunning BFS for ${prefix}graph\n\n"

java -Xmx64g \
    -cp "${libpath}:." \
    SequentialBreadthFirst \
    -n ${numBFSPoints} \
    ${base}/webgraph \
    ${base}/webgraph.bfs \
    ${base}/bfs_startnodes

echo "\n\nRunning BFS for reversed ${prefix}graph\n\n"
if [ "$runRev" = true ]; then    
    if [ ! -d "${revbase}" ]; then
        echo "Reverse graph does not exist!"
        exit 1  
    else
        java -Xmx64g \
            -cp "${libpath}:." \
            SequentialBreadthFirst \
            -n ${numBFSPoints} \
            ${revbase}/webgraph_rev \
            ${revbase}/webgraph_rev.bfs \
            ${base}/bfs_startnodes
    fi
fi

echo "\n\nEverything Completed!\n\n"
