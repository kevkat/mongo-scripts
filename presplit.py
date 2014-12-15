#!/usr/bin/python
from itertools import cycle

# minimum and maximum shard keys, for hashed keys this is +/- 2**64-1
minid   = -2**64-1 
maxid   = 2**64-1
rangeid = abs(minid - maxid)

# database naming info
db         = "tcga_segmentation"
collection = "tcga_segmentation_results"
shard_key  = "uuid"

# number of nodes in sharded cluster
nodes   = 8

# number of total splits desired
splits = 32000

def splitchunks(n, numnodes, numchunks):
	chunklength      = n/numchunks
	chunk_count      = 1
	current_node     = 0
	nodepool         = cycle(range(nodes))
	while chunk_count < numchunks:
		chunkid = chunklength * chunk_count + minid
		chunk_count  += 1
		current_node  = nodepool.next()
		yield 'db.adminCommand( {{ split: "{}.{}", middle: {{ {} : {} }} }} )'.format(db, collection, shard_key, str(chunkid))

for command in splitchunks(rangeid, nodes, 20000):
	print command