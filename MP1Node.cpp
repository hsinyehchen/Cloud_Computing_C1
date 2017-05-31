/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
	for( int i = 0; i < 6; i++ ) {
		NULLADDR[i] = 0;
	}
	this->memberNode = member;
	this->emulNet = emul;
	this->log = log;
	this->par = params;
	this->memberNode->addr = *address;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 * 				This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    Address joinaddr;
    joinaddr = getJoinAddress();

    // Self booting routines
    if( initThisNode(&joinaddr) == -1 ) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }

    if( !introduceSelfToGroup(&joinaddr) ) {
        finishUpThisNode();
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }

    return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
	/*
	 * This function is partially implemented and may require changes
	 */
	int id = *(int*)(&memberNode->addr.addr);
	int port = *(short*)(&memberNode->addr.addr[4]);

	memberNode->bFailed = false;
	memberNode->inited = true;
	memberNode->inGroup = false;
    // node is up!
	memberNode->nnb = 0;
	memberNode->heartbeat = 0;
	memberNode->pingCounter = TFAIL;
	memberNode->timeOutCounter = -1;
    localTime = 0;
    initMemberListTable(memberNode);

    return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
	MessageHdr *msg;
#ifdef DEBUGLOG
    static char s[1024];
#endif

    if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        memberNode->inGroup = true;
    }
    else {
        size_t msgsize = sizeof(MessageHdr) + sizeof(joinaddr->addr) + sizeof(long) + 1;
        msg = (MessageHdr *) malloc(msgsize * sizeof(char));

        // create JOINREQ message: format of data is {struct Address myaddr}
        msg->msgType = JOINREQ;
        memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
        memcpy((char *)(msg+1) + 1 + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));

#ifdef DEBUGLOG
        sprintf(s, "Trying to join...");
        log->LOG(&memberNode->addr, s);
#endif

        // send JOINREQ message to introducer member
        emulNet->ENsend(&memberNode->addr, joinaddr, (char *)msg, msgsize);

        free(msg);
    }

    return 1;

}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode(){
   /*
    * Your code goes here
    */
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
    if (memberNode->bFailed) {
    	return;
    }
    advanceLocalTime();

    // Check my messages
    checkMessages();

    // Wait until you're in the group...
    if( !memberNode->inGroup ) {
    	return;
    }

    // ...then jump in and share your responsibilites!
    nodeLoopOps();

    return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
    void *ptr;
    int size;

    // Pop waiting messages from memberNode's mp1q
    while ( !memberNode->mp1q.empty() ) {
    	ptr = memberNode->mp1q.front().elt;
    	size = memberNode->mp1q.front().size;
    	memberNode->mp1q.pop();
    	recvCallBack((void *)memberNode, (char *)ptr, size);
    }
    return;
}

void MP1Node::procJoinReq(JOINREQ_t* req) {
    Address recvAddr;
    memcpy(&recvAddr, &req->id, sizeof(int)+sizeof(short));
    log->logNodeAdd(&memberNode->addr, &recvAddr);

    MemberListEntry entry(req->id,
                          req->port, 
                          req->heartbeat, 
                          getLocalTime());
    vector<MemberListEntry>& mbLst = memberNode->memberList;
    mbLst.push_back(entry);

    sendJoinRep(req->id, req->port);
}

void MP1Node::sendJoinRep(int rxId, short rxPort) {

    vector<MemberListEntry>& mbLst = memberNode->memberList;
    size_t msgsize = sizeof(JOINREP_t) + mbLst.size()*sizeof(ADDRHRBT_t);

    JOINREP_t *msg = (JOINREP_t *) malloc(msgsize * sizeof(char));
    msg->type = JOINREP;
    memcpy((char*)&msg->addrHr[0], memberNode->addr.addr, sizeof(ADDR_t));
    msg->addrHr[0].hrbeat = memberNode->heartbeat;
    for (int i = 0; i < mbLst.size(); i++) {
        msg->addrHr[1+i].id   = mbLst[i].id;
        msg->addrHr[1+i].port = mbLst[i].port;
        msg->addrHr[1+i].hrbeat = mbLst[i].heartbeat;
    }

    // populate rx address
    Address rxAddr;
    *(int*)&rxAddr.addr[0] = rxId;
    *(short*)&rxAddr.addr[4] = rxPort;

    emulNet->ENsend(&memberNode->addr, &rxAddr, (char *)msg, msgsize);

    free(msg);

}


int MP1Node::findInMBList(vector<MemberListEntry>& mbLst, int id, short port) {
    for (int i = 0; i < mbLst.size(); i++) {
        if (mbLst[i].getid() == id && mbLst[i].getport() == port)
            return i;
    }
    return -1;
}


void MP1Node::procJoinRep(JOINREP_t *rep, int size) {

    int mbNum = (size - sizeof(int))/sizeof(ADDRHRBT_t);
    ADDRHRBT_t *addrHr = &rep->addrHr[0];
    vector<MemberListEntry>& mbLst = memberNode->memberList;

    //cout << "At node"<<memberNode->addr.getAddress();
    //cout << ", tx=("<< addrHr->id<<":"<< addrHr->port<<")"<<endl;
    for (int i = 0; i < mbNum; i++ ) {

        //cout << "(" << addrHr[i].id 
        //     << ":" << addrHr[i].port 
        //     << ":" << addrHr[i].hrbeat << ")";
        Address recvAddr;
        memcpy(&recvAddr, &addrHr[i], sizeof(ADDR_t));
        log->logNodeAdd(&memberNode->addr, &recvAddr);

        MemberListEntry entry(addrHr[i].id,
                              addrHr[i].port,
                              addrHr[i].hrbeat,
                              getLocalTime());
        if (-1 == findInMBList(mbLst, addrHr[i].id, addrHr[i].port))
            mbLst.push_back(entry);
    }
    //cout << endl;
    memberNode->inGroup = true; 
}


/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size ) {
	/*
	 * Your code goes here
	 */
    
    MessageHdr *msg = (MessageHdr*)data;
    switch (msg->msgType) {
    case JOINREQ:
        procJoinReq((JOINREQ_t*)data);
        break;
    case JOINREP:
        procJoinRep((JOINREP_t*)data, size);
        break;
    case GOSSIP:
        procGossip((GOSSIP_t*)data, size);
    case DUMMYLASTMSGTYPE:
    default:
        break; // Do nothing.
    }
}

void MP1Node::sendGossip(int rxId, short rxPort) {

    vector<MemberListEntry>& mbLst = memberNode->memberList;
    size_t msgsize = sizeof(GOSSIP_t) + mbLst.size()*sizeof(ADDRHRBT_t);

    GOSSIP_t *msg = (GOSSIP_t *) malloc(msgsize * sizeof(char));
    msg->type = GOSSIP;
    memcpy((char*)&msg->addrHr[0], memberNode->addr.addr, sizeof(ADDR_t));
    msg->addrHr[0].hrbeat = memberNode->heartbeat;
    for (int i = 1; i < mbLst.size(); i++) {
        msg->addrHr[i].id   = mbLst[i].id;
        msg->addrHr[i].port = mbLst[i].port;
        msg->addrHr[i].hrbeat = mbLst[i].heartbeat;
    }

    // populate rx address
    Address rxAddr;
    *(int*)&rxAddr.addr[0] = rxId;
    *(short*)&rxAddr.addr[4] = rxPort;

    emulNet->ENsend(&memberNode->addr, &rxAddr, (char *)msg, msgsize);

    free(msg);

}


void MP1Node::procGossip(GOSSIP_t *data, int size) {
    cout << "procGossip" << endl;
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {

	/*
	 * Your code goes here
	 */

    vector<MemberListEntry>& mbLst = memberNode->memberList;
    cout <<"T="<<localTime<<", " << *(int*)memberNode->addr.addr << ",";
    for (int i = 0; i < mbLst.size(); i++) {
        cout << "(" << mbLst[i].id 
             << ":" << mbLst[i].port 
             << ":" << mbLst[i].heartbeat
             << ":" << mbLst[i].timestamp << ")";
        sendGossip(mbLst[i].id, mbLst[i].port);
    }
    cout << endl;
    return;
}

/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
	return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
    Address joinaddr;

    memset(&joinaddr, 0, sizeof(Address));
    *(int *)(&joinaddr.addr) = 1;
    *(short *)(&joinaddr.addr[4]) = 0;

    return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode) {
	memberNode->memberList.clear();
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr)
{
    printf("%d.%d.%d.%d:%d \n",  addr->addr[0],addr->addr[1],addr->addr[2],
                                                       addr->addr[3], *(short*)&addr->addr[4]) ;    
}
