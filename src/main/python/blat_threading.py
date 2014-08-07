#!/usr/bin/python
import sys
import struct
import os
import threading
import string
import shutil
from idp_utils import *
from binaidp import log_command

################################################################################

if len(sys.argv) >= 8: #JWDEBUG modified to handle the extra python argument
    blat_threading_pathfilename =  sys.argv[0]
    python_path = sys.argv[1] #JWDEBUG adding this extra agrument to call python
    Nthread1 = int(sys.argv[2])
    blat_option = ' '.join(sys.argv[3:-2])
    SR_pathfilename = sys.argv[-2]
    output_pathfilename = sys.argv[-1]
    
else:
    print("usage: python2.6 blat_threading.py p -t=DNA -q=DNA ~/annotations/hg19/UCSC/hg19/Sequence/WholeGenomeFasta/genome.fa /usr/bin/python intact_SM.fa intact_SM.fa.psl")
    print("or ./blat_threading.py p -t=DNA -q=DNA ~/annotations/hg19/UCSC/hg19/Sequence/WholeGenomeFasta/genome.fa /usr/bin/python intact_SM.fa intact_SM.fa.psl")
    sys.exit(1)

################################################################################
SR_path, SR_filename = GetPathAndName(SR_pathfilename)
output_path, output_filename = GetPathAndName(output_pathfilename)
bin_path2, blat_threading= GetPathAndName(blat_threading_pathfilename)
bin_path1 = bin_path2
################################################################################
SR = open(SR_pathfilename,'r')
SR_NR = 0
for line in SR:
    SR_NR+=1
SR.close()

Nsplitline = 1 + (SR_NR/Nthread1)
if Nsplitline%2==1:
    Nsplitline +=1
ext_ls = [".%s%s" % (string.lowercase[i/26], string.lowercase[i%26]) for i in xrange(Nthread1)]

print "===split SR:==="    
splitSR_cmd = "split -l " + str(Nsplitline) + " " + SR_pathfilename + " " + os.path.join(output_path, SR_filename +".")
print splitSR_cmd
log_command(splitSR_cmd)

##########################################
print "===compress SR.aa:==="    

i=0
T_blat_SR_ls = []
for ext in ext_ls:
    blat_SR_cmd = os.path.join(bin_path1, "blat") + " " + blat_option + ' ' + os.path.join(output_path, SR_filename + ext) + ' ' + os.path.join(output_path, SR_filename + ext + ".psl")
    print blat_SR_cmd
    T_blat_SR_ls.append( threading.Thread(target=log_command, args=(blat_SR_cmd,)) )
    T_blat_SR_ls[i].start()
    i+=1

for T in T_blat_SR_ls:
    T.join()

i=0
T_bestblat_SR_ls = []
for ext in ext_ls:
    bestblat_SR_cmd = python_path + ' ' + os.path.join(bin_path2, "blat_best.py") + " " + os.path.join(output_path, SR_filename + ext + '.psl') +' 5 > ' + os.path.join(output_path, SR_filename + ext + ".bestpsl")
    print bestblat_SR_cmd
    T_bestblat_SR_ls.append( threading.Thread(target=log_command, args=(bestblat_SR_cmd,)) )
    T_bestblat_SR_ls[i].start()
    i+=1

for T in T_bestblat_SR_ls:
    T.join()

with open(output_pathfilename, "w") as output_pathfile_fd:
    for ext in ext_ls:
        with open(os.path.join(output_path, SR_filename + ext + ".bestpsl"), "r") as bestpsl_fd:
            shutil.copyfileobj(bestpsl_fd, output_pathfile_fd)
        os.remove(os.path.join(output_path, SR_filename + ext))
        os.remove(os.path.join(output_path, SR_filename + ext + ".psl"))
