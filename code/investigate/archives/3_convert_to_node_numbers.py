import sys
import os
from os.path import join
import time
import queue
import pickle
import threading
import csv

DICT_PATH = "/dfs/scratch2/dankang/wb_links/url_dict"
FILENAMES_PATH = "/dfs/scratch2/dankang/wb_links/url_files"
##########################################################

##### vvvvvvv CHANGE AS NEEDED vvvvvvvvvvvv  ########
year = 4
num_threads = 1
input_path = "/dfs/scratch2/dankang/wb_links/2004/links_only"
output_file_name = "/dfs/scratch2/dankang/wb_links/2004/arcs"

# FOR TESTING
#DICT_PATH = "/dfs/scratch2/dankang/WebGraph/data/url_dict.pickle"
#FILENAMES_PATH = "/dfs/scratch2/dankang/WebGraph/data/url_files.txt"
#input_path = "/dfs/scratch2/dankang/WebGraph/data/2003_links_only" 
#output_file_name = "/dfs/scratch2/dankang/WebGraph/data/node_graph" 


##### ^^^^^^^ CHANGE AS NEEDED ^^^^^^^^^^^^  ########

def timer(task, start, end):
    hours, rem = divmod(end - start, 3600)
    minutes, seconds = divmod(rem, 60)
    time_text = "[{}]{:0>2}:{:0>2}:{:05.2f}\n".format(task, int(hours),int(minutes),seconds)
    print(time_text)

if (os.path.isfile(output_file_name)):
    print("ERROR: Output file already exists!")
    exit(1)

load_start = time.time()

'''
Load dictionary if it exists
'''
try:
    with open(DICT_PATH, 'rb') as f:
        url_dict = pickle.load(f)
except Exception as e:
    print(e)
    print("Creating new dictionary")
    url_dict = {}

try:
    with open(FILENAMES_PATH, 'r') as f:
        ind = int(f.readline())
        processed_files = [line.rstrip('\n') for line in f]
except FileNotFoundError:
    ind = 0
    processed_files = []

if str(year) in processed_files:
    print("We might have already processed this year! If not, change year value")
    exit(1)

processed_files.append(str(year))

print("ind : {}".format(ind))
print("files: ")
print(processed_files)

threads_start = time.time();
timer("loading dictionary", load_start, threads_start)

file_queue = queue.Queue()
threadLock = threading.Lock()
outputLock = threading.Lock()

'''
UPDATING URL INDEXING
'''
# Update our global history
def update_url_index(record):
    global ind; global threadLock;

    vals = []
    for url in record: # (Src, Dest)
        val = None
        if url in url_dict:
            (val, count)  = url_dict[url]
            url_dict[url] = (val, count | (1 << year))
        else:
            with threadLock:
                val = ind
                url_dict[url] = (ind, 1 << year)
                ind += 1
        vals.append(val)
    return vals

'''
Worker to load the csv file, process the links
'''
def worker():
    global file_queue; global outputLock;
    while True:
        filename = file_queue.get()
        print("Loading {}".format(filename))

        # iterate through the file, update the dictionary accordingly
        output = []
        with open(join(input_path, filename), "r", encoding="utf-8") as f:
            reader = csv.reader(f, delimiter='\t', quoting=csv.QUOTE_NONE)
            for line in reader:
                # just continue if it is invalid format
                if len(line) != 2:
                    print(line)
                    print("Continuing")
                    continue
                vals = update_url_index(line)
                output.append(vals)

        # save to a file
        with outputLock:
            with open(output_file_name, "a+") as f:
                for vals in output:
                    f.write("{}\t{}\n".format(vals[0],vals[1]))
       
        file_queue.task_done()
        
'''
Kick off all threads
'''
for i in range(num_threads):
    t = threading.Thread(target=worker)
    t.daemon = True
    t.start()

'''
Add the files to read to the queue
'''
files = [f for f in os.listdir(input_path) if f != "_SUCCESS" and not f.endswith(".crc")]

for f in files:
    file_queue.put(f)


# Wait for all the threads to be done
file_queue.join()

save_start = time.time()
timer("updating dictionary", threads_start, save_start)

'''
SAVING RESULTS
'''
print("=" * 50)
print("Saving url dictionary")

try:
    with open(DICT_PATH, 'wb') as f:
        pickle.dump(url_dict, f, protocol=pickle.HIGHEST_PROTOCOL);
except Exception as e:
    print("Failed saving url dictionary")

try: 
    with open(FILENAMES_PATH, 'w') as f:
        f.write("{}\n".format(ind))
        for filename in processed_files:
            f.write("{}\n".format(filename))
except Exception as e:
    print("Failed saving filenames")

print("=" * 50)
print("Completed")

all_end = time.time()
timer("saving results", save_start, all_end)

timer("Whole", load_start, all_end)


