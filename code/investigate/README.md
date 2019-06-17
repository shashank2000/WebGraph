# Pipeline

## Basic steps
1. Extract links from archive
  - `1_singleyear.py`
2. Filter the links and hash them
  - `2_filter_links_index_nodes.py` (in single step)
  - `2_part1_filter_links_index_nodes.py`, `2_part2_filter_links_index_nodes.py` (in two steps)
3. Create webgraph and run stats
  - `4_webgraph.sh`: Create a webgraph, run a SCC algorithm on created graph, and generate stats for it.
    Also create a reversed graph, a symmetric graph, and run SCC/Stats for symmetric graph
  
## Further steps
### Domain graph
1. Extract domain level links
  - `3_extract_domains.py`
2. Run same shell script as regular graph `4_webgraph.sh`

### BFS
1. Utilize `5_bfs.sh` script to carry out BFS on regular and reversed graph of a base webgraph

### Stats
1. Utilize `7_get_stats.sh` file to quickly retrieve some key stats from stats file

### Webgraph package analyse
Refer to README file for some detailed call examples
Looking at the API page in the website is helpful for overall summary, but it is 
easier to download the source code to look at different flag options and study code

For specific calls, refer to README file in this directory

