import os

def GetPathAndName(pathfilename):
    return os.path.realpath(os.path.dirname(pathfilename)), os.path.basename(pathfilename)


