name_sync
=========
Author: Hugo Landau <hlandau@devever.net>

This is a proposal for an extension to the Namecoin JSON-RPC interface. The
extension is in the form of a new RPC method, `name_sync`.

New call: name_sync
-------------------

The `name_sync` call takes the following arguments:

  - `block_hash`: A hex-encoded block hash corresponding to `block_height`.
  - `count`: positive integer. Specifies the maximum number of events the client wishes to receive in a single call.
  - `wait`: boolean. Defaults to false.

An Event takes one of the following forms:

  - A `firstupdate` event, indicating the creation of a name.
    The event data is the 2-tuple of the name's name and the name's value.

  - A `update` event, indicating an update made to an existing name.
    The event data is the 2-tuple of the name's name and the name's new value.

  - A `atblock` event, indicating the synchronization position.
    The event data is the 2-tuple of the block hash and the block height.

For each Namecoin block chain (e.g. mainnet) there exists a canonical ordered sequence of Events corresponding to that chain. This sequence is constructed according to the following rules:

  - The sequence begins with an atblock event referring to the genesis block.

  - For each block following the genesis block, in ascending order of height
    and counting only blocks attached to the current chain:

    - Append to the sequence every valid name_firstupdate or name_update
      transaction in the block, in the order that the transactions appear in
      the block.

    - If at least one firstupdate or update event was appended to the sequence
      while processing this block, append an `atblock` event referring to the
      block in question.

Each block thus ends with an `atblock` event, except for blocks having no relevance to the name database.

The purpose of the `name_sync` call is to enable the name database to be exported to formats more suitable for serving DNS, and to enable such derived databases to be updated immediately when name updates occur.

A client using the `name_sync` call stores the following data:

  - A database of names and corresponding values it has received by making
    `name_sync` calls. The height at which a value was seen is also stored.

  - The “current block hash” and “current block height”.

  - A rolling list of the last X block hashes seen. The suggested value of X is 50. In the event that the best chain changes to the extent that more than X blocks are undone, the entire synchronization process must begin again from the genesis block.

Initially, the database is empty. The current block hash is the hash of the genesis block, and the current block height is zero.

The client begins by calling `name_sync` with the current block hash and an arbitrary count value. It receives a sequence of events which constitutes a subsection of the complete sequence of events for the entire block chain.

The server locates the block with the given hash, which must be on the best chain. The server always denies knowledge of any block which is not on the best chain.

If it finds the block, it returns the sequence of events for that block and for every following block, until enough events have been generated to meet or exceed the count value specified by the client. Either all or none of the events for a block must be returned, thus the number of events returned is unlikely to match the count value exactly. Clients must be able to handle receiving a number of events lesser or greater than the number requested.

The client processes the events in the order returned, updating its database in a transactional manner. Whenever it encounters an `atblock` event, it sets the “current block hash” and “current block height” to the values specified in the event and appends that block hash to the rolling list; it does this as part of the transaction, and the transaction is then committed and a new transaction begun. Thus the event sequence is broken into a sequence of (non-overlapping) transactions where each transaction is terminated by the `atblock` event which signifies the end of a block. This allows the client to safely resume synchronization if it is at any point interrupted.

If the server cannot find the block with the given hash on the best chain, this means that the client has processed a block which has since been undone. The server responds with an error. The client must recognise this error condition and proceed as follows:

  - Find the divergence point by following the following process:
    - Let x be the “current block height”.
    - Iterate through the rolling block list backwards (from newest to oldest entry), skipping the first (newest) item. For each item:
      - Decrease x by 1.
      - Call `name_sync` with the given block hash and x as the block height. Specify the count as 0.
      - If `name_sync` fails, continue. If `name_sync` succeeds, the divergence point has been found. The height of the divergence point is x.
    - If the rolling block list is exhausted before the divergence point is found, the rollback process fails and synchronization must occur
      from the genesis block.
  - Identify all name values in the database which have a recorded height higher than the divergence point. For each of these names, issue a `name_show` RPC call and update the value and height of those names as stored in the database with those values. If the RPC call indicates that the name does not exist, delete the name.

    It should be noted that the `name_show` call will return the current value, not the value which was current at the divergence point. However, since the whole point of reverting to the divergence point is to allow the client to catch up with the tip, this is not really an issue.

  - Update the “current block hash” and “current block height”.
  - Truncate the rolling block list to remove any entries after the “current block hash”.
  - Begin synchronization from the new “current block hash” in the normal manner.

If the count is specified as zero, the server returns no events, but whether the call returns with an error indicates the existence of the given block hash at the given height.

If the wait argument is specified as true, the server waits until it has at least one event to return. A client must call the `name_sync` RPC call again to continue long polling. In this case, the count must not be specified as zero.

The client is responsible for managing name expiry using the heights it stores and the “current block height”.
