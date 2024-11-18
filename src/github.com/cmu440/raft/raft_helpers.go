package raft

import (
	"fmt"
	"math/rand"
	"strings"
	"time"
)

// Helpers ------------------------------------------------------------------------------------------------------

// states a Raft peer can be in
type RaftState int

const (
	Follower RaftState = iota
	Candidate
	Leader
)

// type representing the RPC Requests or Responses that can be seen during a servers lifetime.
// each type might trigger a server state change.
type RPCType int

const (
	RequestVoteRequest RPCType = iota
	RequestVoteResponse
	AppendEntriesRequest
	AppendEntriesResponse
)

// struct with the goal is to encode enough information
// to make all state changes possible in a centralized manner.
type RPCInfo struct {
	rType       RPCType
	requestorID int
	termChange  bool
	voteGranted bool
	success     bool
}

// Server -------------------------------------------------------------------------------------------------------

// the main server routine
func (rf *Raft) runServer() {
	for {
		switch rf.state {
		case Follower:
			rf.runFollower()
		case Candidate:
			rf.runCandidate()
		case Leader:
			rf.runLeader()
		}
	}
}

// Follower -----------------------------------------------------------------------------------------------------

// helper function that manages the server during the follower state
func (rf *Raft) runFollower() {
	// init the electionTimer
	rf.resetTimers()
	for {
		select {
		case <-rf.electionTimer.C:
			// election timeout elapses
			// convert to candidate
			rf.mux.Lock()
			rf.logger.Printf("%s : eTimer >>candidate\n", rf.String())
			rf.state = Candidate
			rf.mux.Unlock()
			return
		case info := <-rf.rpcInfoCh:
			rf.mux.Lock()
			// If RPC request or response contains term T > currentTerm:
			// Term change: set currentTerm = T, convert to follower
			if info.termChange {
				rf.logger.Printf("%s : new term >> follower\n", rf.String())
				rf.state = Follower
				rf.mux.Unlock()
				return
			}
			switch info.rType {
			case RequestVoteRequest:
				rf.logger.Printf("%s : [%s:%d, voted: %v]", rf.String(), StringifyRPCType(info.rType), info.requestorID, info.voteGranted)
				// if vote granted, reset timer
				if info.voteGranted {
					rf.resetTimers()
				}
			case AppendEntriesRequest:
				// if AppendEntries recv'd, reset timer
				rf.logger.Printf("%s : [%s:%d]", rf.String(), StringifyRPCType(info.rType), info.requestorID)
				rf.resetTimers()

			case RequestVoteResponse, AppendEntriesResponse:
				// invalid requests with term = Current term, reject
				rf.logger.Printf("%s : Ignored:%s", rf.String(), StringifyRPCType(info.rType))
			}
			rf.mux.Unlock()
		}
	}
}

// Candidate ----------------------------------------------------------------------------------------------------

// helper function that manages the server during the candidate state
func (rf *Raft) runCandidate() {

	// start an election
	// if vote recv'd from majority of servers, become leader
	// if votes recv'd from majority, become leader
	// if AppendEnteiees RPC recv'd from new leader, convert to follower
	// if election timeout elapses, start new election

	rf.startElection()
	numVotes := 1
	for {
		select {
		case <-rf.electionTimer.C:
			// if election timeout elapses,
			// begin a new elecion
			// votedFor == me
			rf.mux.Lock()
			rf.logger.Printf("%s : >>candidate\n", rf.String())
			rf.state = Candidate
			rf.mux.Unlock()
			return

		case info := <-rf.rpcInfoCh:
			rf.mux.Lock()
			// If RPC request or response contains term T > currentTerm:
			// Term change: set currentTerm = T, convert to follower
			if info.termChange {
				rf.logger.Printf("%s : new term >> follower\n", rf.String())
				rf.state = Follower
				rf.mux.Unlock()
				return
			}
			switch info.rType {
			case AppendEntriesRequest:
				// AppendEntries recv'd from leader
				// leaderTerm == currentTerm
				// convert to follower
				rf.votedFor = -1
				rf.logger.Printf("%s : %s >>follower\n", StringifyRPCType(info.rType), rf.String())
				rf.state = Follower
				rf.mux.Unlock()
				return

			case RequestVoteResponse:
				if info.voteGranted {
					numVotes++
					rf.logger.Printf("%s : voteRecvd: %d, numVotes:%d\n", rf.String(), info.requestorID, numVotes)
				}
				// if votes received from majority of servers: become leader
				if numVotes > len(rf.peers)/2 {
					rf.logger.Printf("%s : Majority >>leader\n", rf.String())
					rf.state = Leader
					rf.mux.Unlock()
					return
				}

			case RequestVoteRequest, AppendEntriesResponse:
				// invalid requests with term = Current term, reject
				rf.logger.Printf("%s : Ignored:%s", rf.String(), StringifyRPCType(info.rType))
			}
			rf.mux.Unlock()
		}
	}
}

// helper function to start an election
// increment current term
// vote for self
// reset election timer
// send RequestVote RPC's to all servers
func (rf *Raft) startElection() {
	rf.mux.Lock()
	defer rf.mux.Unlock()
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.resetTimers()
	voteArgs := &RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateID: rf.me,
		LastLogIdx:  len(rf.log) - 1,
		LastLogTerm: rf.log[len(rf.log)-1].Term,
	}
	for i := range rf.peers {
		if i != rf.me {
			reply := &RequestVoteReply{}
			go rf.sendRequestVote(i, voteArgs, reply)
		}
	}
	rf.logger.Printf("%s : ElectionStarted!\n", rf.String())
}

// Leader -------------------------------------------------------------------------------------------------------

// helper function that manages the server during the leader state
func (rf *Raft) runLeader() {

	// set electionTimer to nil
	// initialize nextIndex, matchIndex
	rf.leaderInit()
	// Upon election, send heartbeats to each server
	rf.sendHeartbeats()
	rf.Replicate()
	rf.resetTimers()

	for {
		select {
		case <-rf.heartbeatTimer.C:
			// resend heartbeats during idle periods
			rf.mux.Lock()
			rf.logger.Printf("%s : SendHeartbeats\n", rf.String())
			rf.mux.Unlock()
			rf.HeartbeatOrReplicate()
			rf.resetTimers()

		case info := <-rf.rpcInfoCh:
			rf.mux.Lock()
			// If RPC request or response contains term T > currentTerm:
			// Term change: set currentTerm = T, convert to follower
			if info.termChange {
				rf.logger.Printf("%s : %s:new term >> follower\n", StringifyRPCType(info.rType), rf.String())
				// reset leader volatile state
				rf.nextIndex = nil
				rf.matchIndex = nil
				rf.state = Follower
				rf.mux.Unlock()
				return
			}
			switch info.rType {

			case AppendEntriesResponse, AppendEntriesRequest, RequestVoteRequest, RequestVoteResponse:
				// invalid requests with term = Current term, reject
				rf.logger.Printf("%s : Ignored:%s", rf.String(), StringifyRPCType(info.rType))
			}
			rf.mux.Unlock()
		}
	}

}

// helper function that inits volatile state on leaders
// sets election timer to nil
func (rf *Raft) leaderInit() {
	rf.mux.Lock()
	defer rf.mux.Unlock()
	// will set timer to nil
	rf.resetElectionTimer()
	lastLogIdx := len(rf.log) - 1
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex = append(rf.nextIndex, lastLogIdx+1)
		rf.matchIndex = append(rf.matchIndex, 0)
	}
}

// helper function to send heartbeats (empty entries) to all followers
func (rf *Raft) sendHeartbeats() {
	rf.mux.Lock()
	defer rf.mux.Unlock()
	for i := range rf.peers {
		if i != rf.me {
			nextIndex := max(rf.nextIndex[i], 0)
			args := &AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderID:     rf.me,
				PrevLogIdx:   nextIndex - 1,
				PrevLogTerm:  rf.log[nextIndex-1].Term,
				Entries:      make([]*LogEntry, 0),
				LeaderCommit: rf.commitIndex,
			}
			reply := &AppendEntriesReply{}
			go rf.sendAppendEntries(i, args, reply)
		}
	}
}

// helper function that manages the advances to commitIndex during the leader state
func (rf *Raft) updateCommitIndex() {
	// Iterate from the last log entry downwards to see if a majority has replicated it
	for n := len(rf.log) - 1; n > rf.commitIndex; n-- {
		count := 1 // count self as a vote
		for i := range rf.peers {
			if i != rf.me && rf.matchIndex[i] >= n && rf.log[n].Term == rf.currentTerm {
				count++
			}
		}
		// If a majority has replicated this entry, update commitIndex
		if count > len(rf.peers)/2 {
			rf.commitIndex = n
			rf.logger.Printf("%s : updating commitIndex: %d\n", rf.String(), rf.commitIndex)
			break
		}
	}
}

// after the initial heartbeat append entries
// server alternates between sending heartbeats or
// AppendEntries based on if len(rf.log)-1 >= rf.nextIndex[i]
func (rf *Raft) HeartbeatOrReplicate() {
	rf.mux.Lock()
	defer rf.mux.Unlock()
	if rf.state == Leader {
		for i := range rf.peers {
			if i != rf.me {
				// If last log index ≥ nextIndex for a follower:
				// send AppendEntries RPC with log entries starting at nextIndex
				if len(rf.log)-1 >= rf.nextIndex[i] {
					nextIndex := max(rf.nextIndex[i], 0)
					args := &AppendEntriesArgs{
						Term:         rf.currentTerm,
						LeaderID:     rf.me,
						PrevLogIdx:   nextIndex - 1,
						PrevLogTerm:  rf.log[nextIndex-1].Term,
						Entries:      rf.log[nextIndex:],
						LeaderCommit: rf.commitIndex,
					}
					reply := &AppendEntriesReply{}
					go rf.sendAppendEntries(i, args, reply)
				} else {
					nextIndex := max(rf.nextIndex[i], 0)
					args := &AppendEntriesArgs{
						Term:         rf.currentTerm,
						LeaderID:     rf.me,
						PrevLogIdx:   nextIndex - 1,
						PrevLogTerm:  rf.log[nextIndex-1].Term,
						Entries:      make([]*LogEntry, 0),
						LeaderCommit: rf.commitIndex,
					}
					reply := &AppendEntriesReply{}
					go rf.sendAppendEntries(i, args, reply)
				}
			}
		}
	}
}

// launched as a goroutine by PutCommand
// to send AppendEntries RPC's to all followers
// for log replication
func (rf *Raft) Replicate() {
	rf.mux.Lock()
	defer rf.mux.Unlock()
	if rf.state == Leader {
		for i := range rf.peers {
			if i != rf.me {
				// If last log index ≥ nextIndex for a follower:
				// send AppendEntries RPC with log entries starting at nextIndex
				if len(rf.log)-1 >= rf.nextIndex[i] {
					nextIndex := max(rf.nextIndex[i], 0)
					args := &AppendEntriesArgs{
						Term:         rf.currentTerm,
						LeaderID:     rf.me,
						PrevLogIdx:   nextIndex - 1,
						PrevLogTerm:  rf.log[nextIndex-1].Term,
						Entries:      rf.log[nextIndex:],
						LeaderCommit: rf.commitIndex,
					}
					reply := &AppendEntriesReply{}
					go rf.sendAppendEntries(i, args, reply)
				}
			}
		}
	}
}

// State Machine ------------------------------------------------------------------------------------------------

// helper function that is launched as a goroutine to periotically apply to state machine
// If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine
func (rf *Raft) runApply() {
	for {
		rf.mux.Lock()

		// Wait until there's something new to apply
		if rf.commitIndex <= rf.lastApplied {
			rf.mux.Unlock()
			time.Sleep(10 * time.Millisecond)
			continue
		}

		// Apply all pending commands in order
		rf.lastApplied += 1
		rf.logger.Printf("%s : Apply: lastApplied: %d, commitIndex: %d\n", rf.String(), rf.lastApplied, rf.commitIndex)
		command := ApplyCommand{
			Index:   rf.lastApplied,
			Command: rf.log[rf.lastApplied].Command,
		}

		rf.mux.Unlock()
		rf.applyCh <- command
	}
}

// Timers -------------------------------------------------------------------------------------------------------

// Constants to set election and heartbeat timers
const (
	MinElectionTimeout = 400 * time.Millisecond
	MaxElectionTimeout = 800 * time.Millisecond
	HeartbeatTimeout   = 100 * time.Millisecond
)

// Generate a random election time duration based on preset lower and upper bounds
func randomElectionTimeout() time.Duration {
	return time.Duration(rand.Intn(int(MaxElectionTimeout-MinElectionTimeout))) + MinElectionTimeout
}

// helper function that resets all timers based on raft state
func (rf *Raft) resetTimers() {
	rf.resetElectionTimer()
	rf.resetHeartbeatTimer()
}

// does nothing in leader state,
// creates a NewTimer if nil in the follower | candidate state
// resets  the electionTimer if non-nil in the follower | candidate state
func (rf *Raft) resetElectionTimer() {

	if rf.state == Leader {
		rf.electionTimer = nil
		return
	}
	if rf.electionTimer == nil {
		rf.electionTimer = time.NewTimer(randomElectionTimeout())
		return
	}
	if !rf.electionTimer.Stop() {
		// Drain the channel if the timer has already fired
		select {
		case <-rf.electionTimer.C:
		default:
		}
	}
	rf.electionTimer.Reset(randomElectionTimeout())
}

// if hearbeat Timer is nil, init one
// else, stop and reset
func (rf *Raft) resetHeartbeatTimer() {

	if rf.state != Leader {
		rf.heartbeatTimer = nil
		return
	}

	if rf.heartbeatTimer == nil {
		rf.heartbeatTimer = time.NewTimer(HeartbeatTimeout)
		return
	}
	if !rf.heartbeatTimer.Stop() {
		select {
		case <-rf.heartbeatTimer.C:
		default:
		}
	}
	rf.heartbeatTimer.Reset(HeartbeatTimeout)
}

// Stringify's --------------------------------------------------------------------------------------------------

// string representation of state
func (rf *Raft) StringifyState() string {
	state := ""
	switch rf.state {
	case Follower:
		state = "follower"
	case Candidate:
		state = "candidate"
	case Leader:
		state = "leader"
	}
	return state
}

// creates a string representation for an AppendEntriesArgs instance
func StringifyAppendEntriesArgs(args *AppendEntriesArgs) string {
	return fmt.Sprintf(
		"AppendEntriesArgs{Term:%d, LeaderID:%d, PrevLogIdx:%d, PrevLogTerm:%d, Entries:%s, LeaderCommit:%d}",
		args.Term,
		args.LeaderID,
		args.PrevLogIdx,
		args.PrevLogTerm,
		StringifyLogEntries(args.Entries),
		args.LeaderCommit,
	)

}

// creates a string representation for an []*LogEntry instance
func StringifyLogEntries(entries []*LogEntry) string {
	var sb strings.Builder
	sb.WriteString("[")

	for i, entry := range entries {
		if entry != nil {
			// Create a string for the current LogEntry
			entryStr := fmt.Sprintf("{Cmd:%v,Term:%d}", entry.Command, entry.Term)
			sb.WriteString(entryStr)

			// Add a comma and space if this is not the last element
			if i < len(entries)-1 {
				sb.WriteString(", ")
			}
		}
	}
	sb.WriteString("]")
	return sb.String()
}

// string representations of RPC requests/responses
func StringifyRPCType(t RPCType) string {
	typ := ""
	switch t {
	case RequestVoteRequest:
		typ = "RequestVoteRequest"
	case RequestVoteResponse:
		typ = "RequestVoteResponse"
	case AppendEntriesRequest:
		typ = "AppendEntriesRequest"
	case AppendEntriesResponse:
		typ = "AppendEntriesResponse"
	}
	return typ
}

// string representation of a raft server
func (rf *Raft) String() string {
	str := fmt.Sprintf("[me:%d, term:%d, state:%s, votedFor:%d, commitIdx:%d, logs:%s]", rf.me, rf.currentTerm, rf.StringifyState(), rf.votedFor, rf.commitIndex, StringifyLogEntries(rf.log))
	//str := fmt.Sprintf("[me:%d, term:%d, state:%s, commitIdx:%d]", rf.me, rf.currentTerm, rf.StringifyState(), rf.commitIndex)
	return str

}
