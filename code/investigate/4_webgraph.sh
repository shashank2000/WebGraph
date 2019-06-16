#!/bin/bash

year="$1"
prefix="domain_"
#prefix=""

root="/dfs/scratch2/dankang/wb_links/${1}"
base="${root}/${prefix}webgraph"
revbase="${root}/${prefix}webgraph_rev"
ubase="${root}/${prefix}webgraph_u"
libpath="/dfs/scratch2/dankang/WebGraph/lib/webgraph/*"

echo "\n\nCreating ${prefix}graph\n\n"
if [ ! -d "${base}" ]; then
    mkdir ${base}

    tsvDirectory="${root}/${prefix}hash_links.tsv/"
    #tsvFile=$(find ${root}/${prefix}hash_links.tsv/ -type f -iname "*.csv")
    
    java -cp "${libpath}:." \
        ScatteredArcsASCIIHexGraphDirectory \
        ${tsvDirectory} \
        ${base}/webgraph \
        
else
    echo "[WARNING] ${prefix}graph already created - passing. If this is not true, remove ${base} and try again"
fi

echo "\n\nRunning SCC on ${prefix}graph\n\n"

webgraphFile=$(find ${base}/ -type f -iname "webgraph.graph")
if [ -z "$webgraphFile" ]; then
    echo "Webgraph was not created!"
    exit 1
else
    
    sccFile=$(find ${base}/ -type f -iname "webgraph.scc")

    if [ -z "$sccFile" ]; then
        java -cp "${libpath}" \
            it.unimi.dsi.webgraph.algo.StronglyConnectedComponents \
            -s -r -b \
            ${base}/webgraph \
            ${base}/webgraph
    else
        echo "[WARNING] Scc already created - passing. If this is not true, check ${base}"
    fi
fi

echo "\n\nRunning Stats on SCC of ${prefix}graph\n\n"

if [ ! -d "${base}/stats" ]; then
    mkdir ${base}/stats

    sccFile=$(find ${base}/ -type f -iname "webgraph.scc")

    if [ -z "$sccFile" ]; then
        echo "SCC was not created!"
        exit 1
    else
        java -cp "${libpath}" \
        it.unimi.dsi.webgraph.Stats \
        -s \
        ${base}/webgraph \
        ${base}/stats/stats
    fi
else
    echo "[WARNING] Stats already computed - passing. If this is not true, remove ${base}/stats and try again"
fi

echo "\n\nCreating reversed ${prefix}graph\n\n"
if [ ! -d "${revbase}" ]; then
    mkdir ${revbase}

    java -Xmx256g \
        -cp "${libpath}" \
        it.unimi.dsi.webgraph.Transform \
        transpose \
        ${base}/webgraph \
        ${revbase}/webgraph_rev
else
    echo "[WARNING] Reversed ${prefix}graph already created - passing. If this is not true, remove ${revbase} and try again"
fi

# Running scc and stats on reversed graph is not that helpful information

#echo "\n\nRunning SCC on reversed ${prefix}graph\n\n"
#
#revWebgraphFile=$(find ${revbase}/ -type f -iname "webgraph_rev.graph")
#if [ -z "$revWebgraphFile" ]; then
#    echo "Reversed webgraph was not created!"
#    exit 1
#else
#    revsccFile=$(find ${revbase}/ -type f -iname "webgraph_rev.scc")
#
#    if [ -z "$revsccFile" ]; then
#        java -cp "${libpath}" \
#            it.unimi.dsi.webgraph.algo.StronglyConnectedComponents \
#            -s -r -b \
#            ${revbase}/webgraph_rev \
#            ${revbase}/webgraph_rev
#    else
#        echo "[WARNING] Scc already created - passing. If this is not true, check ${revbase}"
#    fi
#
#fi
#

#echo "\n\nRunning stats on SCC of reversed ${prefix}graph\n\n"
#
#if [ ! -d "${revbase}/stats" ]; then
#    mkdir ${revbase}/stats
#
#    revsccFile=$(find ${revbase}/ -type f -iname "webgraph_rev.scc")
#
#    if [ -z "$revsccFile" ]; then
#        echo "Reversed SCC was not created!"
#        exit 1
#    else
#        java -cp "${libpath}" \
#        it.unimi.dsi.webgraph.Stats \
#        -s \
#        ${revbase}/webgraph_rev \
#        ${revbase}/stats/stats
#    fi
#else
#    echo "[WARNING] Stats already computed - passing. If this is not true, remove ${revbase}/stats and try again"
#fi

echo "\n\nCreating symmetric ${prefix}graph\n\n"
if [ ! -d "${ubase}" ]; then
    mkdir ${ubase}

    java -Xmx256g \
        -cp "${libpath}" \
        it.unimi.dsi.webgraph.Transform \
        symmetrize \
        ${base}/webgraph \
        ${revbase}/webgraph_rev \
        ${ubase}/webgraph_u
else
    echo "[WARNING] Symmetric ${prefix}graph already created - passing. If this is not true, remove ${ubase} and try again"
fi

echo "\n\nRunning SCC on symmetric ${prefix}graph\n\n"

uWebgraphFile=$(find ${ubase}/ -type f -iname "webgraph_u.graph")
if [ -z "$uWebgraphFile" ]; then
    echo "Symmetric webgraph was not created!"
    exit 1
else
    usccFile=$(find ${ubase}/ -type f -iname "webgraph_u.scc")

    if [ -z "$usccFile" ]; then
        java -cp "${libpath}" \
            it.unimi.dsi.webgraph.algo.StronglyConnectedComponents \
            -s -r -b \
            ${ubase}/webgraph_u \
            ${ubase}/webgraph_u
    else
        echo "[WARNING] Scc already created - passing. If this is not true, check ${ubase}"
    fi
fi

echo "\n\nRunning stats on SCC of symmetric ${prefix}graph\n\n"

if [ ! -d "${ubase}/stats" ]; then
    mkdir ${ubase}/stats

    usccFile=$(find ${ubase}/ -type f -iname "webgraph_u.scc")

    if [ -z "$usccFile" ]; then
        echo "Symmetric SCC was not created!"
        exit 1
    else
        java -cp "${libpath}" \
        it.unimi.dsi.webgraph.Stats \
        -s \
        ${ubase}/webgraph_u \
        ${ubase}/stats/stats
    fi
else
    echo "[WARNING] Stats already computed - passing. If this is not true, remove ${ubase}/stats and try again"
fi

echo "\n\nEverything Completed!\n\n"
