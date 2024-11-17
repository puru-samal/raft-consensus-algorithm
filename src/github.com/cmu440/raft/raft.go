//
// raft.go
// =======
// Write your code in this file
// We will use the original version of all other
// files for testing
//

package raft

//
// API
// ===
// This is an outline of the API that your raft implementation should
// expose.
//
// rf = NewPeer(...)
//   Create a new Raft server.
//
// rf.PutCommand(command interface{}) (index, term, isleader)
//   PutCommand agreement on a new log entry
//
// rf.GetState() (me, term, isLeader)
//   Ask a Raft peer for "me" (see line 58), its current term, and whether it thinks it
//   is a leader
//
// ApplyCommand
//   Each time a new entry is committed to the log, each Raft peer
//   should send an ApplyCommand to the service (e.g. tester) on the
//   same server, via the applyCh channel passed to NewPeer()
//

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"sync"
	"time"

	"github.com/cmu440/rpc"
)

// Set to false to disable debug logs completely
// Make sure to set kEnableDebugLogs to false before submitting
const kEnableDebugLogs = false

// Set to true to log to stdout instead of file
const kLogToStdout = true

// Change this to output logs to a different directory
const kLogOutputDir = "./raftlogs/"

// ApplyCommand
// ========
//
// As each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyCommand to the service (or
// tester) on the same server, via the applyCh passed to NewPeer()
type ApplyCommand struct {
	Index   int
	Command interface{}
}

// each entry contains command for state machine
// and term when entry was recv'd by leader (first idx is 1)
type LogEntry struct {
	Command interface{}
	Term    int
}

// Raft struct
// ===========
//
// A Go object implementing a single Raft peer
type Raft struct {
	mux   sync.Mutex       // Lock to protect shared access to this peer's state
	peers []*rpc.ClientEnd // RPC end points of all peers
	me    int              // this peer's index into peers[]
	// You are expected to create reasonably clear log files before asking a
	// debugging question on Edstem or OH. Use of this logger is optional, and
	// you are free to remove it completely.
	logger *log.Logger // We provide you with a separate logger per peer.

	// TODO - Your data here (2A, 2B).
	// Look at the Raft paper's Figure 2 for a description of what
	// state a Raft peer should maintain

	// Persistent State ---------------------------------------------------------------------------
	currentTerm int         // Last term server has seen, (init 0, monotonically increasing)
	votedFor    int         // candidateID that recv'd vote in current term (or null(-1) if none)
	log         []*LogEntry // log entries
	// Volatile State (all) -----------------------------------------------------------------------
	commitIndex int // index of highest log entry known to be committed (init 0, monotonically increasing)
	lastApplied int // index of highest log entry applied to state machine (init 0, monotonically increasing)
	// Volatile State (leader) --------------------------------------------------------------------
	// reinit after each election
	nextIndex  []int // for each server, idx of the next log entry to send to that server (init to leader last log idx+1)
	matchIndex []int // for each server, idx of highest log entry known to be replicated on server (init 0, monotonically increasing)
	state      RaftState

	// Timers -------------------------------------------------------------------------------------
	electionTimer  *time.Timer // timer that begins leader elections [300 - 500ms]
	heartbeatTimer *time.Timer // becomes non-nil when a server becomes a leader [100ms]

	// Channels -----------------------------------------------------------------------------------
	applyCh       chan ApplyCommand // channel to send commited log entries to
	applyNotifyCh chan int          // channel to notify that a log entry is available to commit
	rpcInfoCh     chan *RPCInfo     // channel to send state information during rpc calls/responses to manage server state
}

// GetState
// ==========
// Return "me", current term and whether this peer
// believes it is the leader
func (rf *Raft) GetState() (int, int, bool) {
	var me int
	var term int
	var isleader bool
	// TODO - Your code here (2A)
	rf.mux.Lock()
	me = rf.me
	term = rf.currentTerm
	isleader = rf.state == Leader
	rf.mux.Unlock()
	return me, term, isleader
}

// RequestVoteArgs
// ===============
// # Please note: Field names must start with capital letters!
type RequestVoteArgs struct {
	// TODO - Your data here (2A, 2B)
	Term        int // candidate's term
	CandidateID int // candidate requesting vote
	LastLogIdx  int // idx of candidate's last log entry
	LastLogTerm int // term of candidate's last log entry
}

// RequestVoteReply
// ================
// # Please note: Field names must start with capital letters!
type RequestVoteReply struct {
	// TODO - Your data here (2A)
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate recieved vote
}

// RequestVote
// ===========
// 1. invoked by candidates to gather votes
// 2. candidates send args and and empty reply
// 3. followers update reply with their term and vote
// 4. an RPCInfo is sent to the main routine to handle state changes
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// TODO - Your code here (2A, 2B)

	rf.mux.Lock()
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	// 1. reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		rf.mux.Unlock()
		return
	}

	// if currentTerm is stale,
	// update term
	// reset votedFor aka convert to follower
	termChange := false
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		termChange = true
	}

	// 2. check if voting condition is met
	lastLogIdx := len(rf.log) - 1
	lastLogTerm := rf.log[len(rf.log)-1].Term

	isCandidateEligible := rf.votedFor == -1 || rf.votedFor == args.CandidateID
	isLogUpToDate := lastLogTerm < args.LastLogTerm || (lastLogTerm == args.LastLogTerm && lastLogIdx <= args.LastLogIdx)

	// if voted for is null or candidateid and
	// candidate’s log is at least as up-to-date as receiver’s log
	// grant vote
	if isCandidateEligible && isLogUpToDate {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateID
	}
	rf.mux.Unlock()

	// send an RPCInfo to main server routine because:
	// 1. term might require updating => state change might be required
	info := &RPCInfo{
		rType:       RequestVoteRequest,
		requestorID: args.CandidateID,
		termChange:  termChange,
		voteGranted: reply.VoteGranted,
	}

	rf.rpcInfoCh <- info
}

// sendRequestVote
// ===============
//
// Example code to send a RequestVote RPC to a server.
//
// server int -- index of the target server in
// rf.peers[]
//
// args *RequestVoteArgs -- RPC arguments in args
//
// reply *RequestVoteReply -- RPC reply
//
// The types of args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers)
//
// The rpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost
//
// Call() sends a request and waits for a reply.
//
// If a reply arrives within a timeout interval, Call() returns true;
// otherwise Call() returns false
//
// Thus Call() may not return for a while.
//
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply
//
// Call() is guaranteed to return (perhaps after a delay)
// *except* if the handler function on the server side does not return
//
// Thus there
// is no need to implement your own timeouts around Call()
//
// Please look at the comments and documentation in ../rpc/rpc.go
// for more details
//
// If you are having trouble getting RPC to work, check that you have
// capitalized all field names in the struct passed over RPC, and
// that the caller passes the address of the reply struct with "&",
// not the struct itself
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if ok {
		// detect term changes
		// send to main server routine to handle state changes
		termChange := false
		rf.mux.Lock()
		if reply.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			rf.votedFor = -1
			termChange = true
		}
		rf.mux.Unlock()
		info := &RPCInfo{
			rType:       RequestVoteResponse,
			requestorID: server,
			termChange:  termChange,
			voteGranted: reply.VoteGranted,
		}
		rf.rpcInfoCh <- info
	}
	return ok
}

// AppendEntriesArgs
// ===============
// # Please note: Field names must start with capital letters!
type AppendEntriesArgs struct {
	Term         int         // Leader's Term
	LeaderID     int         // so follower can redirect clients
	PrevLogIdx   int         // idx of log entry immediately preceding new ones
	PrevLogTerm  int         // term of prevLogIdx entry
	Entries      []*LogEntry // log entries to store (empty for heartbeat)
	LeaderCommit int         // Leaders commit index
}

// AppendEntriesReply
// ===============
// # Please note: Field names must start with capital letters
type AppendEntriesReply struct {
	Term          int  // currentTerm for leader to update itself
	Success       bool // true if follower contained entry matching prevLogIdc and prevLogTerm
	ConflictTerm  int  // term of conflicting entry (-1 if none)
	ConflictIndex int  // first index storing conflict term
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mux.Lock()

	rf.logger.Printf("%s: %s\n", rf.String(), StringifyAppendEntriesArgs(args))

	reply.Term = rf.currentTerm
	// 1. reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		rf.mux.Unlock()
		return
	}

	// if currentTerm is stale,
	// update term
	// reset votedFor aka convert to follower
	termChange := false
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		termChange = true
	}

	// 2. reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
	reply.Success = true
	if len(rf.log) <= args.PrevLogIdx || rf.log[args.PrevLogIdx].Term != args.PrevLogTerm {
		reply.Success = false
		rf.logger.Printf("success: %v\n", reply.Success)
		// Optimization: Collect information about the conflicting term
		if len(rf.log) > args.PrevLogIdx {

			// Find the first index of the conflicting term
			conflictTerm := rf.log[args.PrevLogIdx].Term
			firstIndexOfConflictTerm := args.PrevLogIdx
			for firstIndexOfConflictTerm > 0 && rf.log[firstIndexOfConflictTerm-1].Term == conflictTerm {
				firstIndexOfConflictTerm--
			}
			reply.ConflictTerm = conflictTerm
			reply.ConflictIndex = firstIndexOfConflictTerm

		} else {
			// There's no entry at PrevLogIdx, so the conflict is with a missing entry
			reply.ConflictTerm = -1 // Indicating no specific term found
			reply.ConflictIndex = len(rf.log)
		}
		rf.mux.Unlock()
		return
	}

	// len(rf.log) > args.PrevLogIdx && rf.log[args.PrevLogIdx].Term == args.PrevLogTerm
	// 3. If an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow it
	for i, newEntry := range args.Entries {
		idx := args.PrevLogIdx + 1 + i
		if idx < len(rf.log) {
			// If there's a conflict in terms, delete the existing entry and all that follow it
			if rf.log[idx].Term != newEntry.Term {
				rf.log = rf.log[:idx]
				break
			}
		}
	}

	// 4. Append any new entries not already in the log
	for i, newEntry := range args.Entries {
		idx := args.PrevLogIdx + 1 + i
		// Append new entries if they are not already in the log
		if idx >= len(rf.log) {
			rf.log = append(rf.log, newEntry)
		}
	}

	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		lastNewEntryIdx := len(rf.log) - 1
		if len(args.Entries) == 0 {
			lastNewEntryIdx = args.PrevLogIdx
		}
		rf.commitIndex = min(args.LeaderCommit, lastNewEntryIdx)
	}

	rf.mux.Unlock()

	rType := AppendEntriesRequest
	info := &RPCInfo{
		rType:       rType,
		requestorID: args.LeaderID,
		termChange:  termChange,
		success:     reply.Success,
	}

	rf.rpcInfoCh <- info

	// If commitIndex > lastApplied:
	// increment lastApplied,
	// apply log[lastApplied] to state machine
	select {
	case rf.applyNotifyCh <- 1:
	default:
		// If the channel is full, don't block
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok {
		// detect term changes
		// send to main server routine to handle state changes
		termChange := false
		rf.mux.Lock()
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			termChange = true
		}
		rf.mux.Unlock()
		info := &RPCInfo{
			rType:           AppendEntriesResponse,
			termChange:      termChange,
			success:         reply.Success,
			requestorID:     server,
			appendEntryArgs: args,
			conflictTerm:    reply.ConflictTerm,
			conflictIndex:   reply.ConflictIndex,
		}
		rf.rpcInfoCh <- info
	}
	return ok
}

// PutCommand
// =====
//
// The service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log
//
// If this server is not the leader, return false.
//
// Otherwise start the agreement and return immediately.
//
// There is no guarantee that this command will ever be committed to
// the Raft log, since the leader may fail or lose an election
//
// The first return value is the index that the command will appear at
// if it is ever committed
//
// The second return value is the current term.
//
// The third return value is true if this server believes it is
// the leader
func (rf *Raft) PutCommand(command interface{}) (int, int, bool) {
	// TODO - Your code here (2B)
	rf.mux.Lock()
	index := len(rf.log)
	term := rf.currentTerm
	isLeader := rf.state == Leader
	launchReplicate := false
	// If command received from client: append entry to local log
	if isLeader {
		newEntry := &LogEntry{
			Command: command,
			Term:    term,
		}
		rf.log = append(rf.log, newEntry)
		launchReplicate = true
	}
	rf.mux.Unlock()
	// initiate replication process
	if launchReplicate {
		go rf.Replicate()
	}
	return index, term, isLeader
}

// Stop
// ====
//
// The tester calls Stop() when a Raft instance will not
// be needed again
//
// You are not required to do anything
// in Stop(), but it might be convenient to (for example)
// turn off debug output from this instance
func (rf *Raft) Stop() {
	// TODO - Your code here, if desired
	rf.mux.Lock()
	defer rf.mux.Unlock()
	rf.logger.SetOutput(io.Discard)
}

// NewPeer
// ====
//
// The service or tester wants to create a Raft server.
//
// The port numbers of all the Raft servers (including this one)
// are in peers[]
//
// This server's port is peers[me]
//
// All the servers' peers[] arrays have the same order
//
// applyCh
// =======
//
// applyCh is a channel on which the tester or service expects
// Raft to send ApplyCommand messages
//
// NewPeer() must return quickly, so it should start Goroutines
// for any long-running work
func NewPeer(peers []*rpc.ClientEnd, me int, applyCh chan ApplyCommand) *Raft {
	rf := &Raft{
		// 2A
		currentTerm:    0,
		votedFor:       -1,
		state:          Follower,
		electionTimer:  nil,
		heartbeatTimer: nil,
		rpcInfoCh:      make(chan *RPCInfo),

		// TODO: 2B
		log: []*LogEntry{
			{Command: 0, Term: 0},
		},
		commitIndex:   0,
		lastApplied:   0,
		nextIndex:     nil,
		matchIndex:    nil,
		applyNotifyCh: make(chan int, 1),
	}
	rf.peers = peers
	rf.me = me
	rf.applyCh = applyCh

	if kEnableDebugLogs {
		peerName := peers[me].String()
		logPrefix := fmt.Sprintf("%s ", peerName)
		if kLogToStdout {
			rf.logger = log.New(os.Stdout, peerName, log.Lmicroseconds|log.Lshortfile)
		} else {
			err := os.MkdirAll(kLogOutputDir, os.ModePerm)
			if err != nil {
				panic(err.Error())
			}
			logOutputFile, err := os.OpenFile(fmt.Sprintf("%s/%s.txt", kLogOutputDir, logPrefix), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
			if err != nil {
				panic(err.Error())
			}
			rf.logger = log.New(logOutputFile, logPrefix, log.Lmicroseconds|log.Lshortfile)
		}
		rf.logger.Println("logger initialized")
	} else {
		rf.logger = log.New(ioutil.Discard, "", 0)
	}

	// TODO - Your initialization code here (2A, 2B)

	// The main server routine
	// the server starts of as a follower
	go rf.runServer()
	go rf.runApply()
	return rf
}
