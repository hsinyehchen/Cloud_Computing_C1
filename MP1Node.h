/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Header file of MP1Node class.
 **********************************/

#ifndef _MP1NODE_H_
#define _MP1NODE_H_

#include "stdincludes.h"
#include "Log.h"
#include "Params.h"
#include "Member.h"
#include "EmulNet.h"
#include "Queue.h"

/**
 * Macros
 */
#define TREMOVE 20
#define TFAIL 5

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Message Types
 */
enum MsgTypes{
    JOINREQ,
    JOINREP,
    GOSSIP,
    DUMMYLASTMSGTYPE
};

/**
 * STRUCT NAME: MessageHdr
 *
 * DESCRIPTION: Header and content of a message
 */
typedef struct MessageHdr {
	enum MsgTypes msgType;
}MessageHdr;

#pragma pack(push, 1)
typedef struct {
    int id;
    short port;
}ADDR_t;

typedef struct {
    int id;
    short port;
    long hrbeat;
}ADDRHRBT_t;

typedef struct {
    enum MsgTypes type;
    ADDRHRBT_t addrHr[1];
}GOSSIP_t;

typedef struct {
    enum MsgTypes type;
    ADDRHRBT_t addrHr[1];
}JOINREP_t;

typedef struct {
    enum MsgTypes type;
    int id;
    short port;
    char rfu;
    long heartbeat;
}JOINREQ_t;

#pragma pack(pop)

/**
 * CLASS NAME: MP1Node
 *
 * DESCRIPTION: Class implementing Membership protocol functionalities for failure detection
 */
class MP1Node {
private:
	EmulNet *emulNet;
	Log *log;
	Params *par;
	Member *memberNode;
	char NULLADDR[6];
        long localTime;
        vector<MemberListEntry> watchList = vector<MemberListEntry>();
public:
	MP1Node(Member *, Params *, EmulNet *, Log *, Address *);
	Member * getMemberNode() {
		return memberNode;
	}
        void advanceLocalTime(void) {localTime++;};
        long getLocalTime(void) {return localTime;}
	int recvLoop();
	static int enqueueWrapper(void *env, char *buff, int size);
	void nodeStart(char *servaddrstr, short serverport);
	int initThisNode(Address *joinaddr);
	int introduceSelfToGroup(Address *joinAddress);
	int finishUpThisNode();
	void nodeLoop();
	void checkMessages();
	bool recvCallBack(void *env, char *data, int size);
	void nodeLoopOps();
	int isNullAddress(Address *addr);
	Address getJoinAddress();
	void initMemberListTable(Member *memberNode);
	void printAddress(Address *addr);
        void procJoinReq(JOINREQ_t *req);
        void procJoinRep(JOINREP_t *rep, int size);
        int findInMBList(vector<MemberListEntry>& mbLst, int id, short port);
        void procGossip(GOSSIP_t *data, int size);
        void sendJoinRep(int rxId, short rxPort);
        void sendGossip(int rxId, short rxPort);
        void checkMember();
	virtual ~MP1Node();
};

#endif /* _MP1NODE_H_ */
