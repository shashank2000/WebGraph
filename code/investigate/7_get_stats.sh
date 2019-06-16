#!/bin/bash

# This file goes through files and prints out stats we might be curious about
# Feel free to add/modify anything
# Note there is no error checking: if a file is missing, it will just fail

# Params
year="$1"
#prefix="domain_"
prefix=""
#suffix="_prev"
suffix=""

# environ definition - change as needed
root="/dfs/scratch2/dankang/wb_links/${1}"
base="${root}/${prefix}webgraph${suffix}"
revbase="${root}/${prefix}webgraph_rev${suffix}"
ubase="${root}/${prefix}webgraph_u${suffix}"
codepath="/dfs/scratch2/dankang/WebGraph/code/investigate"
libpath="/dfs/scratch2/dankang/WebGraph/lib/webgraph/*"

regStatFile="${base}/stats/stats.stats"
revStatFile="${revbase}/stats/stats.stats"
uStatFile="${ubase}/stats/stats.stats"

# look at regular stats
nodes=$(cat ${regStatFile} | grep nodes | cut -d "=" -f 2)
arcs=$(cat ${regStatFile} | grep arcs | cut -d "=" -f 2)
maxout=$(cat ${regStatFile} | grep -m 1 maxoutdegree | cut -d "=" -f 2)
avgout=$(cat ${regStatFile} | grep -m 1 avgoutdegree | cut -d "=" -f 2 | cut -c1-5)
maxin=$(cat ${regStatFile} | grep -m 1 maxindegree | cut -d "=" -f 2)
scc=$(cat ${regStatFile} | grep -m 1 sccs | cut -d "=" -f 2)
maxscc=$(cat ${regStatFile} | grep -m 1 maxsccsize | cut -d "=" -f 2)
percmaxscc=$(cat ${regStatFile} | grep -m 1 percmaxscc | cut -d "=" -f 2 | cut -c1-5)
dangling=$(cat ${regStatFile} | grep -m 1 dangling | cut -d "=" -f 2)
percdangling=$(cat ${regStatFile} | grep -m 1 percdangling | cut -d "=" -f 2 | cut -c1-5)

# look at undirected stats
u_arcs=$(cat ${uStatFile} | grep arcs | cut -d "=" -f 2)
u_maxout=$(cat ${uStatFile} | grep -m 1 maxoutdegree | cut -d "=" -f 2)
u_avgout=$(cat ${uStatFile} | grep -m 1 avgoutdegree | cut -d "=" -f 2 | cut -c1-5)
u_scc=$(cat ${uStatFile} | grep -m 1 sccs | cut -d "=" -f 2)
u_maxscc=$(cat ${uStatFile} | grep -m 1 maxsccsize | cut -d "=" -f 2)
u_percmaxscc=$(cat ${uStatFile} | grep -m 1 percmaxscc | cut -d "=" -f 2 | cut -c1-5)
u_dangling=$(cat ${uStatFile} | grep -m 1 dangling | cut -d "=" -f 2)
u_percdangling=$(cat ${uStatFile} | grep -m 1 percdangling | cut -d "=" -f 2 | cut -c1-5)

echo "==${year} ${prefix}graph stats==\n\
nodes: ${nodes}\n\
===========Regular graph============\n\
    arcs: ${arcs}\n\
    maxout: ${maxout}\n\
    avgout: ${avgout}\n\
    maxin: ${maxin}\n\
    scc: ${scc}\n\
    maxscc: ${maxscc}(${percmaxscc}%)\n\
    dangling: ${dangling}(${percdangling})\n\
===========Undirected graph============\n\
    arcs: ${u_arcs}\n\
    maxout: ${u_maxout}\n\
    avgout: ${u_avgout}\n\
    scc: ${u_scc}\n\
    maxscc: ${u_maxscc}(${u_percmaxscc}%)\n\
    dangling: ${u_dangling}(${u_percdangling})\n\
"

