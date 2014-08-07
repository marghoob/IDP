#!/usr/bin/python

import sys
import os
import threading
import shutil
from idp_utils import *

#Main**************************************************************************#
def main():
    # Read input parameters
    bin_path,command = GetPathAndName(sys.argv[0])
    input_filename = sys.argv[1]
    output_filename = sys.argv[2]
    num_threads = int(sys.argv[3])
    python_path = sys.argv[4]
    penalty_filename = ''
    if (len(sys.argv) > 5):
        penalty_filename = sys.argv[5]
    
    input_file = open(input_filename, 'r' )
    header = input_file.readline()
    input_files = []
    for thread_idx in range(num_threads):
        input_files.append(open(input_filename + '.' + str(thread_idx), 'w'))
        input_files[-1].write(header)

        
    thread_idx = 0
    while True:
        line = input_file.readline()
        if line == "": 
            break
        num_isoforms = int(line.split()[1])
        input_files[thread_idx].write(line)
        
        for i in xrange(6 + num_isoforms + 2):
            input_files[thread_idx].write(input_file.readline())
    
        thread_idx = (thread_idx + 1) % num_threads
        
    for thread_idx in range(num_threads):
        input_files[thread_idx].close()
    input_file.close()
    
    ##############################
    threads_list = []
    for thread_idx in range(num_threads):
        cmd = python_path + " " + bin_path + 'MLE_regions.py ' + input_filename + '.' + str(thread_idx) + ' ' + output_filename + '.' + str(thread_idx)
        if (penalty_filename != ''):
            cmd += ' ' + penalty_filename
        print cmd
        threads_list.append( threading.Thread(target=log_command, args=(cmd,)) )
        threads_list[thread_idx].start()

    for thread in threads_list:
        thread.join()
        
    with open(output_filename, "w") as output_file_fd:
        for thread_idx in range(num_threads):
            with open("%s.%d" % (output_filename, thread_idx), "r") as src_fd:
                shutil.copyfileobj(src_fd, output_file_fd)
            os.remove("%s.%d" % (output_filename, thread_idx))
            os.remove("%s.%d" % (input_filename, thread_idx))
        

if __name__ == '__main__':
    main()
