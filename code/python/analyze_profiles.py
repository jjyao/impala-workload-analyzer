import sys
sys.path.append('gen-py')

import zlib
import base64
from RuntimeProfile.ttypes import *
from profile_analyzer import ProfileAnalyzer
from thrift.protocol import TCompactProtocol

print sys.argv[1]

tag = sys.argv[2]
analyzer = ProfileAnalyzer()
with open(sys.argv[1], 'r') as profileFile:
    for line in profileFile:
        profile = line.split(" ")[2]
        profileData = zlib.decompress(base64.b64decode(profile))
        memoryBuffer = TTransport.TMemoryBuffer(profileData)
        compactProtocol = TCompactProtocol.TCompactProtocol(memoryBuffer)
        profileTree = TRuntimeProfileTree()
        profileTree.read(compactProtocol)
        analyzer.analyze(profileTree, tag)
