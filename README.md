# Cloud_Computing_C1
This is repository for programming assignment of Coursera's Cloud Computing by University of Illinois at Urbana-Champaign

This assignment is about implementing a membership protocol. The membership protocol will run on top of EmulNet in a peer- to-peer (P2P) layer, but below an App layer. 

The membership protocol must satisfy: 
  1. Completeness all the time: every non-faulty process must detect every node
  2. Accuracy of failure detection when there are no message losses and message delays are small. 

When there are message losses, completeness must be satisfied and accuracy must be high. It must achieve all of these even under simultaneous multiple failures.

The implementation is only in MP1Node.cpp and MP1Node.h. All the other files remains untouched as provided by the course content. The membership implemented here is the GOSSIP style. The key components of GOSSIP is as follows,

1. At each cycle, the node update the heartbeat and randomly send the membership table to some nodes in the membership table.
2. When receiving the GOSSIP package, the node compares the package's heartbeat to the membership table. If the heartbeat is newer, it updates the heartbeat and timestamp of that member in the table.
3. At each cycle, the node check the timestamps of membership table. If the timeout is reached, the member is removed from the membership table and added to the failure table. Then the node will check through the failure table. If another timeout is reached, it removes the node from the failure table. This can prevent the failing node gets bounced back and forth in the membership table.
