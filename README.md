# Cloud_Computing_C1
This is repository for programming assignment of Coursera's Cloud Computing Concepts Part1/Part2 by University of Illinois at Urbana-Champaign

This assignment is about implementing a membership protocol and fault-tolerant key-value store. The protocols will run on top of EmulNet in a peer- to-peer (P2P) layer, but below an App layer. The implementation is only in MP1Node.cpp, MP2Node.cpp and header files. All the other files remains untouched as provided by the course content.

The membership protocol must satisfy under message losses and multiple failures:
  1. Completeness all the time: every non-faulty process must detect every node
  2. Accuracy of failure detection when there are no message losses and message delays are small. 

Here I implement the GOSSIP style to achieve the above properties. The key components of GOSSIP is as follows,
* At each cycle, the node update the heartbeat and randomly send the membership table to some nodes.
* When receiving the GOSSIP message, the node compares the package's heartbeat to the membership table. If the heartbeat is newer, it updates the heartbeat and timestamp of that member in the table.
* At each cycle, the node check the timestamps. If the timeout is reached, the member is moved to the failure table. If another timeout is reached, it removes the node from the membership. This can prevent message bounced back and forth in the membership table.

The fault-tolerant key-value store will provide the functionalities under node failures:
  1. A key-value store supporting CRUD operations (Create, Read, Update, Delete). 
  2. Load-balancing (via a consistent hashing ring to hash both servers and keys). 
  3. Fault-tolerance up to two failures (by replicating each key three times to three successive nodes in the ring, starting from the first node at or to the clockwise of the hashed key). 
  4. Quorum consistency level for both reads and writes (at least two replicas). 
  5. Stabilization after failure (recreate three replicas after failure).

In this assignment, each key-value pair has 3 replicas in the hash ring, and the hash ring is formed from the membership table maintained by the GOSSIP protocol. Each node sees the same hash ring, so the key-value pair can be located by the hash function. When a node (coordinator) receives CRUD requests, it sends message to all the 3 nodes who (should) have the replicas. Once 2 or more (quorum) replies are received, it forwards the result back to the upper layer. If nodes crashed, the membership protocol can detect the failure and hash ring will be updated. The other nodes will replicate the key-value pairs stored in the failing nodes, so there will still be 3 replicas in the system.
