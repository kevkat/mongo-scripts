#!/usr/bin/python
from itertools import cycle

# minimum and maximum shard keys, for hashed keys this is +/- 2**63
minid   = -2**63
maxid   = 2**63
rangeid = abs(minid - maxid)

# database naming info
db         = "tcga_segmentation"
collection = "tcga_segmentation_results"
shard_key  = "uuid"

# shard information
shards      = 10
shardprefix = "shard"

# number of total splits desired
splits = 30000

# function to generate mongo db commands
def splitchunks(n, numshards, numchunks):
	chunklength      = n/numchunks
	chunk_count      = 1
	current_shard     = 0
	shardpool         = cycle(range(shards))
	while chunk_count < numchunks:
		chunkid = chunklength * chunk_count + minid
		chunk_count  += 1
		current_shard  = str(shardpool.next()).zfill(4) # mongo shard notation is shard0000, shard0001, etc. so fill leading zeroes
		yield 'db.adminCommand( {{ split: "{0}.{1}", middle: {{ {2} : NumberLong("{3}") }} }} ) \n' \
		'db.adminCommand( {{ moveChunk: "{0}.{1}", find: {{ {2}: "{3}" }}, to: "{4}{5}" }} )'.format(db, collection, shard_key, str(chunkid), shardprefix, current_shard)

# splitchunks() returns an iterable list of commands, so we need to print them
for command in splitchunks(rangeid, shards, splits):
	print command