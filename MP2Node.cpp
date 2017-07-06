/**********************************
 * FILE NAME: MP2Node.cpp
 *
 * DESCRIPTION: MP2Node class definition
 **********************************/
#include "MP2Node.h"

/**
 * constructor
 */
MP2Node::MP2Node(Member *memberNode, Params *par, EmulNet * emulNet, Log * log, Address * address) {
	this->memberNode = memberNode;
	this->par = par;
	this->emulNet = emulNet;
	this->log = log;
	ht = new HashTable();
	this->memberNode->addr = *address;
}

/**
 * Destructor
 */
MP2Node::~MP2Node() {
	delete ht;
	delete memberNode;
}

/**
 * FUNCTION NAME: updateRing
 *
 * DESCRIPTION: This function does the following:
 * 				1) Gets the current membership list from the Membership Protocol (MP1Node)
 * 				   The membership list is returned as a vector of Nodes. See Node class in Node.h
 * 				2) Constructs the ring based on the membership list
 * 				3) Calls the Stabilization Protocol
 */
void MP2Node::updateRing() {
	/*
	 * Implement this. Parts of it are already implemented
	 */
	vector<Node> curMemList;
        vector<Node> oldRing(ring.begin(), ring.end());
        int oldIdx = ringIdx;

	/*
	 *  Step 1. Get the current membership list from Membership Protocol / MP1
	 */
	curMemList = getMembershipList();

        curMemList.push_back(Node(this->memberNode->addr));
        /*cout << "Node(" <<this->memberNode->addr.getAddress() << ") >>> ";
        for (auto n : curMemList) {
            Address * addr = n.getAddress();
            cout << addr->getAddress() << ",";
        }
        cout << endl;*/

	/*
	 * Step 2: Construct the ring
	 */
        // Sort the list based on the hashCode
	sort(curMemList.begin(), curMemList.end());
        ring.assign(curMemList.begin(), curMemList.end());
        updateNeighbors();

        /*cout << "Ring(" <<this->memberNode->addr.getAddress() << ") ::: ";
        for (auto n : ring) {
            Address * addr = n.getAddress();
            cout << addr->getAddress() << ",";
        }
        cout << endl;
        cout << "ringIdx = " << ringIdx;
        cout << ", " << haveReplicasOf[1].nodeAddress.getAddress();
        cout << "<" << haveReplicasOf[0].nodeAddress.getAddress();
        cout << "-" << hasMyReplicas[0].nodeAddress.getAddress();
        cout << ">" << hasMyReplicas[1].nodeAddress.getAddress() << endl;*/

	/*
	 * Step 3: Run the stabilization protocol IF REQUIRED
	 */
	// Run stabilization protocol if the hash table size is greater than zero and if there has been a changed in the ring
        if (ring.size() < oldRing.size())
            stabilizationProtocol(oldRing, oldIdx);

        cout << "Node(" << memberNode->addr.getAddress() << ") => ";
        for (auto ele : ht->hashTable)
            cout << "(" << ele.first << ":" << ele.second << ":" << mp_Rep[ele.first]<< "),";
        cout << endl;
}

/**
 * FUNCTION NAME: getMemberhipList
 *
 * DESCRIPTION: This function goes through the membership list from the Membership protocol/MP1 and
 * 				i) generates the hash code for each member
 * 				ii) populates the ring member in MP2Node class
 * 				It returns a vector of Nodes. Each element in the vector contain the following fields:
 * 				a) Address of the node
 * 				b) Hash code obtained by consistent hashing of the Address
 */
vector<Node> MP2Node::getMembershipList() {
	unsigned int i;
	vector<Node> curMemList;
	for ( i = 0 ; i < this->memberNode->memberList.size(); i++ ) {
		Address addressOfThisMember;
		int id = this->memberNode->memberList.at(i).getid();
		short port = this->memberNode->memberList.at(i).getport();
		memcpy(&addressOfThisMember.addr[0], &id, sizeof(int));
		memcpy(&addressOfThisMember.addr[4], &port, sizeof(short));
		curMemList.emplace_back(Node(addressOfThisMember));
	}
	return curMemList;
}

/**
 * FUNCTION NAME: hashFunction
 *
 * DESCRIPTION: This functions hashes the key and returns the position on the ring
 * 				HASH FUNCTION USED FOR CONSISTENT HASHING
 *
 * RETURNS:
 * size_t position on the ring
 */
size_t MP2Node::hashFunction(string key) {
	std::hash<string> hashFunc;
	size_t ret = hashFunc(key);
	return ret%RING_SIZE;
}

/**
 * FUNCTION NAME: clientCreate
 *
 * DESCRIPTION: client side CREATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientCreate(string key, string value) {
	/*
	 * Implement this
	 */

    vector<Node> nodeLst(findNodes(key));

    mpMsgRec[g_transID] = KY_RPY(0, CREATE, key, value);
    Message msg(g_transID++, this->memberNode->addr, CREATE, key, value, PRIMARY);
    cout << "clientCreate("<< memberNode->addr.getAddress() <<"): key = " << key << ", value=" << value << endl;
    for (auto n : nodeLst) {
        cout << "Replicate "<<msg.replica<<" to " << n.nodeAddress.getAddress() << endl;
        if (memcmp(memberNode->addr.addr,n.nodeAddress.addr, 6*sizeof(char)) == 0) {
            //cout << "Create nodeLst is Coordinator!!!"<< key << "::" << value << endl;

            mpMsgRec[g_transID].rx_rpy += (int)createKeyValue(key, value, msg.replica);
        }
        else {
            emulNet->ENsend(&memberNode->addr, &n.nodeAddress, msg.toString());
        }
        msg.replica = (ReplicaType)((char)msg.replica + 1);
    }
}

/**
 * FUNCTION NAME: clientRead
 *
 * DESCRIPTION: client side READ API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientRead(string key){
	/*
	 * Implement this
	 */
    //cout << "clientRead, key = " << key << endl;
    vector<Node> nodeLst(findNodes(key));

    //if (ht->count(key) > 0)
    //    cout << "Coordinator has the KEEEEYYYYYYYYYY!!!" << endl;

    mpMsgRec[g_transID] = KY_RPY(0, READ, key, "");
    Message msg(g_transID++, this->memberNode->addr, READ, key);

    for (auto n : nodeLst) {
        if (memcmp(memberNode->addr.addr,n.nodeAddress.addr, 6*sizeof(char)) == 0) {
            //cout << "nodeLst is Coordinator!!!" << endl;
            mpMsgRec[g_transID].rx_rpy++;
        }
        else {
            //cout << "TX="<<msg.toString() << "  , to "<< n.nodeAddress.getAddress() << endl;
            emulNet->ENsend(&memberNode->addr, &n.nodeAddress, msg.toString());
        }
    }
}

/**
 * FUNCTION NAME: clientUpdate
 *
 * DESCRIPTION: client side UPDATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientUpdate(string key, string value){
	/*
	 * Implement this
	 */
    vector<Node> nodeLst(findNodes(key));

    mpMsgRec[g_transID] = KY_RPY(0, UPDATE, key, value);
    Message msg(g_transID++, this->memberNode->addr, UPDATE, key, value, PRIMARY);
    cout << "clientUpdate("<< memberNode->addr.getAddress() <<"): key = " << key << ", value=" << value << endl;
    for (auto n : nodeLst) {
        cout << "Update "<<msg.replica<<" to " << n.nodeAddress.getAddress() << endl;
        if (memcmp(memberNode->addr.addr,n.nodeAddress.addr, 6*sizeof(char)) == 0) {
            //cout << "Create nodeLst is Coordinator!!!"<< key << "::" << value << endl;

            mpMsgRec[g_transID].rx_rpy += (int)updateKeyValue(key, value, msg.replica);
        }
        else {
            emulNet->ENsend(&memberNode->addr, &n.nodeAddress, msg.toString());
        }
        msg.replica = (ReplicaType)((char)msg.replica + 1);
    }
}

/**
 * FUNCTION NAME: clientDelete
 *
 * DESCRIPTION: client side DELETE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientDelete(string key){
	/*
	 * Implement this
	 */
}

/**
 * FUNCTION NAME: createKeyValue
 *
 * DESCRIPTION: Server side CREATE API
 * 			   	The function does the following:
 * 			   	1) Inserts key value into the local hash table
 * 			   	2) Return true or false based on success or failure
 */
bool MP2Node::createKeyValue(string key, string value, ReplicaType replica) {
	/*
	 * Implement this
	 */
	// Insert key, value, replicaType into the hash table
    
    ht->create(key, value);
    mp_Rep[key] = replica;
    return true;
}


void MP2Node::handleCreateMessage(Message message) {
    
    bool success = createKeyValue(message.key, message.value, message.replica);

    if (success)
        log->logCreateSuccess(&memberNode->addr, 
                          false, 
                          message.transID, 
                          message.key, message.value);
    else
        log->logCreateFail(&memberNode->addr, 
                           false, 
                           message.transID, 
                           message.key, message.value);

    Message txMsg(message.transID, memberNode->addr, REPLY, success);
    emulNet->ENsend(&memberNode->addr, &message.fromAddr, txMsg.toString());
    //cout << "handleCreateMessage: " << message.key << ", " << (int)mp_Rep[message.key] << endl;
}


void MP2Node::handleReplyMessage(Message message) {

    int id = message.transID;

    map<int, KY_RPY>::iterator it = mpMsgRec.find(id);

    if (it == mpMsgRec.end())
        return;

    if (message.success)
        mpMsgRec[id].rx_rpy++;

    if (mpMsgRec[id].rx_rpy >= 2) {

        MessageType type = mpMsgRec[id].type;

        if (type == CREATE) {
            
            log->logCreateSuccess(&memberNode->addr, 
                                  true, 
                                  id, 
                                  mpMsgRec[id].key, mpMsgRec[id].value);

            mpMsgRec.erase(it);
        }
        else if (type == UPDATE) {
            log->logUpdateSuccess(&memberNode->addr, 
                                  true, 
                                  id, 
                                  mpMsgRec[id].key, mpMsgRec[id].value);

            mpMsgRec.erase(it);
        }
        else if (type == DELETE) {

        }
    }
}


/**
 * FUNCTION NAME: readKey
 *
 * DESCRIPTION: Server side READ API
 * 			    This function does the following:
 * 			    1) Read key from local hash table
 * 			    2) Return value
 */
string MP2Node::readKey(string key) {
	/*
	 * Implement this
	 */
	// Read key from local hash table and return value

    return ht->read(key);
}

void MP2Node::handleReadMessage(Message message) {
    int id = message.transID;
    if (ht->count(message.key) > 0) {
        string value = readKey(message.key);

        Message txMsg(message.transID, memberNode->addr, value);
        emulNet->ENsend(&memberNode->addr, &message.fromAddr, txMsg.toString());

        log->logReadSuccess(&memberNode->addr, false, id, message.key, value);
    }
    else {
        Message txMsg(message.transID, memberNode->addr, REPLY, false);
        emulNet->ENsend(&memberNode->addr, &message.fromAddr, txMsg.toString());

        log->logReadFail(&memberNode->addr, false, id, message.key);
    }
    
}


void MP2Node::handleReadreplyMessage(Message message) {
    int id = message.transID;
    map<int, KY_RPY>::iterator it = mpMsgRec.find(id);
    if (it == mpMsgRec.end())
        return;

    if (message.success)
        mpMsgRec[id].rx_rpy++;

    if (mpMsgRec[id].rx_rpy >= 2) {

        MessageType type = mpMsgRec[id].type;
        assert(type == READ);
        if (type == READ) {
            cout << "READ success" << endl;
            log->logReadSuccess(&memberNode->addr, 
                                  true, 
                                  id, 
                                  mpMsgRec[id].key, message.value);

            mpMsgRec.erase(it);
        }
    }
}

/**
 * FUNCTION NAME: updateKeyValue
 *
 * DESCRIPTION: Server side UPDATE API
 * 				This function does the following:
 * 				1) Update the key to the new value in the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::updateKeyValue(string key, string value, ReplicaType replica) {
	/*
	 * Implement this
	 */
	// Update key in local hash table and return true or false
    if (ht->update(key, value)) {
        mp_Rep[key] = replica;
        return true;
    }
    else
        return false;
}


void MP2Node::handleUpdateMessage(Message message) {
    bool success = updateKeyValue(message.key, message.value, message.replica);

    if (success)
        log->logUpdateSuccess(&memberNode->addr, 
                          false, 
                          message.transID, 
                          message.key, message.value);
    else
        log->logUpdateFail(&memberNode->addr, 
                           false, 
                           message.transID, 
                           message.key, message.value);

    Message txMsg(message.transID, memberNode->addr, REPLY, success);
    emulNet->ENsend(&memberNode->addr, &message.fromAddr, txMsg.toString());
}

/**
 * FUNCTION NAME: deleteKey
 *
 * DESCRIPTION: Server side DELETE API
 * 				This function does the following:
 * 				1) Delete the key from the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::deletekey(string key) {
	/*
	 * Implement this
	 */
	// Delete the key from the local hash table
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: This function is the message handler of this node.
 * 				This function does the following:
 * 				1) Pops messages from the queue
 * 				2) Handles the messages according to message types
 */
void MP2Node::checkMessages() {
	/*
	 * Implement this. Parts of it are already implemented
	 */
	char * data;
	int size;

	/*
	 * Declare your local variables here
	 */

	// dequeue all messages and handle them
	while ( !memberNode->mp2q.empty() ) {
		/*
		 * Pop a message from the queue
		 */
		data = (char *)memberNode->mp2q.front().elt;
		size = memberNode->mp2q.front().size;
		memberNode->mp2q.pop();

		string message(data, data + size);
                cout << memberNode->addr.getAddress() << " RX=" << message << endl;
                Message rxMsg(message);
		/*
		 * Handle the message types here
		 */
                switch(rxMsg.type) {
                case CREATE:
                    handleCreateMessage(rxMsg);
                    break;
                case UPDATE:
                    handleUpdateMessage(rxMsg);
                    break;
                case REPLY:
                    handleReplyMessage(rxMsg);
                    break;
                case READ:
                    handleReadMessage(rxMsg);
                    break;
                case READREPLY:
                    handleReadreplyMessage(rxMsg);
                    break;
                }

	}

	/*
	 * This function should also ensure all READ and UPDATE operation
	 * get QUORUM replies
	 */
}

/**
 * FUNCTION NAME: findNodes
 *
 * DESCRIPTION: Find the replicas of the given keyfunction
 * 				This function is responsible for finding the replicas of a key
 */
vector<Node> MP2Node::findNodes(string key) {
	size_t pos = hashFunction(key);
	vector<Node> addr_vec;
	if (ring.size() >= 3) {
		// if pos <= min || pos > max, the leader is the min
		if (pos <= ring.at(0).getHashCode() || pos > ring.at(ring.size()-1).getHashCode()) {
			addr_vec.emplace_back(ring.at(0));
			addr_vec.emplace_back(ring.at(1));
			addr_vec.emplace_back(ring.at(2));
		}
		else {
			// go through the ring until pos <= node
			for (int i=1; i<ring.size(); i++){
				Node addr = ring.at(i);
				if (pos <= addr.getHashCode()) {
					addr_vec.emplace_back(addr);
					addr_vec.emplace_back(ring.at((i+1)%ring.size()));
					addr_vec.emplace_back(ring.at((i+2)%ring.size()));
					break;
				}
			}
		}
	}
	return addr_vec;
}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: Receive messages from EmulNet and push into the queue (mp2q)
 */
bool MP2Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), this->enqueueWrapper, NULL, 1, &(memberNode->mp2q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue of MP2Node
 */
int MP2Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}
/**
 * FUNCTION NAME: stabilizationProtocol
 *
 * DESCRIPTION: This runs the stabilization protocol in case of Node joins and leaves
 * 				It ensures that there always 3 copies of all keys in the DHT at all times
 * 				The function does the following:
 *				1) Ensures that there are three "CORRECT" replicas of all the keys in spite of failures and joins
 *				Note:- "CORRECT" replicas implies that every key is replicated in its two neighboring nodes in the ring
 */
void MP2Node::stabilizationProtocol(vector<Node>& oldRing, int oldIdx) {
	/*
	 * Implement this
	 */
    
    vector<Node> crashNode;
    vector<int>  crashIdx;
    for (int i = 0, j = 0; i < ring.size() && j < oldRing.size();) {
        if (memcmp(ring[i].nodeAddress.addr, oldRing[j].nodeAddress.addr, sizeof(char)*6) == 0) {
            i++;
            j++;
        }
        else {
            crashNode.push_back(oldRing[j]);
            crashIdx.push_back(j);
            j++;
        }
    }
    
    /*cout << "Old Ring = ";
    for (Node n : oldRing)
        cout << "(" << n.nodeAddress.getAddress() << ") ";
    cout << endl;
    cout << "Ring = ";
    for (Node n : ring)
        cout << "(" << n.nodeAddress.getAddress() << ") ";
    cout << endl;*/

    bool pre1 = false, pre2 = false, post1 = false, post2 = false;

    for (int i = 0; i < crashNode.size(); i++) {
        cout << memberNode->addr.getAddress() <<"- Node Missing: " << crashNode[i].nodeAddress.getAddress() << endl;
        if (crashIdx[i] == (oldIdx + 1)%oldRing.size())
            post1 = true;
        else if(crashIdx[i] == (oldIdx + 2)%oldRing.size())
            post2 = true;
        else if(crashIdx[i] == (oldIdx - 1 + oldRing.size())%oldRing.size())
            pre1 = true;
        else if(crashIdx[i] == (oldIdx - 2 + oldRing.size())%oldRing.size())
            pre2 = true;
    }

    for (auto ele : mp_Rep) {
        if (ele.second == PRIMARY) {
            string key = ele.first, value = ht->read(ele.first);
            if (post1 && post2) {
                Message msg1(g_transID, memberNode->addr, CREATE, key, value, SECONDARY);
                emulNet->ENsend(&memberNode->addr, &hasMyReplicas[0].nodeAddress, msg1.toString());
                Message msg2(g_transID++, memberNode->addr, CREATE, key, value, TERTIARY);
                emulNet->ENsend(&memberNode->addr, &hasMyReplicas[1].nodeAddress, msg2.toString());
            }
            else if (post1) {
                Message msg1(g_transID, memberNode->addr, UPDATE, key, value, SECONDARY);
                emulNet->ENsend(&memberNode->addr, &hasMyReplicas[0].nodeAddress, msg1.toString());
                Message msg2(g_transID++, memberNode->addr, CREATE, key, value, TERTIARY);
                emulNet->ENsend(&memberNode->addr, &hasMyReplicas[1].nodeAddress, msg2.toString());
            }
            else if (post2) {
                Message msg2(g_transID++, memberNode->addr, CREATE, key, value, TERTIARY);
                emulNet->ENsend(&memberNode->addr, &hasMyReplicas[1].nodeAddress, msg2.toString());
            }
        }
    }
}

void MP2Node::updateNeighbors() {
    int idx = 0;
    while (memcmp(ring[idx].nodeAddress.addr, this->memberNode->addr.addr, 6*sizeof(char)) != 0)
        idx++;
    ringIdx = idx;

    hasMyReplicas.resize(2);
    haveReplicasOf.resize(2);
    //cout << "  idx=" << idx << endl;
    for (int i = 0; i < 2; i++) {
        int id0 = (idx+1+i)%ring.size();
        int id1 = (idx-1-i < 0) ? (idx-1-i+ring.size()) : idx-1-i;
        hasMyReplicas[i] = ring[id0];
        haveReplicasOf[i] = ring[id1];
        //cout << "id0 = " << id0 << endl;
        //cout << "id1 = " << id1 << endl;
    }
    
}
