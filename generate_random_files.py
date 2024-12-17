import logging
logging.basicConfig(level=logging.DEBUG)


import numpy as np
from pathlib import Path
import random
import threading
import time
import datetime

ROOT_LOCATION:list[str]=["e:"]

ALL_CHARS="ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"

LARGE_SIZE=int(50E6) # 50 MB
SMALL_SIZE=10**3 # 1 KB
g_size_count:int=0
TARGET_SIZE=10**9 # 1GB
DIR_PROPORTION=0.15
MAX_DEPTH=5
BIG_PROPORTION=0.01
ITEMS_PER_DIRECTORY=40
g_file_count=0
g_largest_size=0


def generate_file_size(min_size=SMALL_SIZE, max_size=100_000_000):
    """
    Randomly generates a file size (in bytes) following a skewed distribution
    to mimic typical file size distribution on a Windows PC.

    Args:
        min_size (int): Minimum file size in bytes (default 1 byte).
        max_size (int): Maximum file size in bytes (default 50 MB).

    Returns:
        int: A randomly generated file size in bytes.
    """
    # Parameters for the log-normal distribution
    mean = 1    # The mean of the underlying normal distribution
    sigma = 4    # The standard deviation (controls spread)
    
    # Generate a log-normal distributed value
    file_size = random.lognormvariate(mean, sigma)
    
    # Scale to the desired range and clip
    file_size = int(min(max_size, max(min_size, file_size)))
    
    return file_size

def make_name(is_dir:bool=False)->str:
    filename="".join([random.choice(ALL_CHARS) for _ in range(20)])
    if is_dir:
        return filename
    return filename+".data"

def write_file(size:int,path:list[str]):
    """
        Writes to the path a new file with a randomly generated name
        where path is expressed as a list of directory names

    """
    dtype = np.uint32
    random_data=np.random.randint(0,high=2**32, size=size//4, dtype=dtype) # writes 32 bit words
    
    with open(Path(*path,make_name(is_dir=False)),"wb") as outfile:
        outfile.write(random_data)

def fill_directory(path:list[str],depth:int):
    """
        Used recusively to fill directories with stuff
        Puts 10 things in each directory roughly 8 being files, 2 being subdirectories
        Files will be small 99% of the time
    """
    global g_size_count,g_file_count,g_largest_size
    for _ in range(ITEMS_PER_DIRECTORY):
        if random.random()<DIR_PROPORTION and depth<MAX_DEPTH: # Randomly make a subdirectory instead
            new_directory=make_name(is_dir=True)
            Path(*path,new_directory).mkdir(parents=True,exist_ok=True)
            fill_directory(path+[new_directory],depth=depth+1)
        else:
            size=generate_file_size() # LARGE_SIZE if random.random()<BIG_PROPORTION else SMALL_SIZE
            if size>g_largest_size:
                g_largest_size=size
            g_size_count+=size
            write_file(size,path)
            g_file_count+=1
        if g_size_count>TARGET_SIZE:
            print(f"Stopped after, writing a total of {g_size_count} bytes")
            break

def monitor_progress():
    """ 
    run as daemon thread, monitors progress
    """
    start_time=datetime.datetime.now()
    while True:
        time.sleep(1)
        run_for=(datetime.datetime.now()-start_time).total_seconds()
        rate=g_size_count/run_for
        time_left_s=int((TARGET_SIZE-g_size_count)/rate)
        print(f"Running for: {run_for}s    Written: {g_size_count:,} bytes    % Complete: {g_size_count/TARGET_SIZE*100:.1f}   Remaining time: {time_left_s} s    file_count: {g_file_count}    ave.size: {g_size_count/g_file_count:,.0f}   largest: {g_largest_size:,}")



if __name__=="__main__":
    Path(*ROOT_LOCATION).mkdir(parents=True,exist_ok=True)

    t=threading.Thread(target=monitor_progress,name="Progress",daemon=True)
    t.start()


    fill_directory(ROOT_LOCATION,0)
    print(f"Finished, wrote a total of {g_size_count} bytes")

