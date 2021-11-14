from random import random
import time
import sys
import threading

pid = int(sys.argv[1])
n = int(sys.argv[2])
print(f"Starting server {pid}", file=sys.stderr)

# while True:
#     print(f"SEND {(pid+1)%n} PING {pid}", flush=True)
#     line = sys.stdin.readline()
#     if line is None:
#         break
#     print(f"Got {line.strip()}", file=sys.stderr)
#     time.sleep(2)

# print(f"Pinger {pid} done", file=sys.stderr)

l=threading.Lock()


class Raft:
    def __init__(self,n,pid):
        self.state='"FOLLOWER"'
        print(f'STATE',f'state={self.state}')

        self.n=n
        self.pid=pid
        self.leader=None

        self.log=[]#modify2
        self.commitId = 0
        self.matchId=dict()
        self.nextId=dict()
##        self.rpcDue=dict()
##        self.hbDue=dict()
        for i in range(n):
            self.matchId[i]=0
            self.nextId[i]=1
##            self.rpcDue[i]=0
##            self.hbDue[i]=0
        #end of modify2
        
        self.votedFor=pid
        self.term=0
        print(f'STATE',f'term={self.term}')

        self.voteGranted=[False]*n
        self.ELECTION_TIMEOUT=1

        self.timer=None
        self.resetTimer()

        self.heartbeat=None

    def logTerm(self, log, ind):
        if ind<1 or ind>len(log):
            return 0
        return log[ind-1].term
    
    def resetTimer(self):
        
        #fix concurrency issue
        
        self.timer=threading.Timer(self.ELECTION_TIMEOUT*(1+random()),self.timeoutHandlerThread)
        self.timer.start()


    def stepdown(self,term):
        self.term=term
        print(f'STATE',f'term={self.term}')
        
        self.votedFor=None
        self.voteGranted=[False]*self.n
        self.state='"FOLLOWER"'
        print(f'STATE',f'state={self.state}')
        #todo: check this in heartbeat thread
        #  if self.heartbeat:
        #     self.heartbeat.cancel()
        

    def send(self,destpid,*args):
##modify3
##self.send(i,'RequestVotes',self.term,lastLogTerm,LastLogId)
##self.send(i,'AppendEntries',self.term, prevId, prevTerm, entry, self.commitId)
##        print('SEND',destpid,*args,flush=True)
        if len(args) < 2:
            return
        msgtype = args[1]
        if msgtype == 'RequestVotes':
            #todo, don't need so many parameters
            print('SEND',destpid,args,flush=True)
        elif msgtype == 'AppendEntries':
            #todo
            print('SEND',destpid,args,flush=True)
            
        
    def becomeLeader(self):
        self.state='"LEADER"'
        print(f'STATE',f'state={self.state}')

        self.leader=self.pid
        print(f'STATE',f'leader={self.leader}')

        #modify3
        for i in range(n):
            if i == self.pid:
                continue
            self.nextId[i] = len(self.log)+1
            
        self.heartbeat=threading.Thread(target=self.heartbeatThread,args=(self.term,))
        self.heartbeat.start()

    def processmsg(self,msg):
        msg=msg.split()
                
        #modify2, LOG msg
        if msg[0]=='LOG':
            content=msg[1]
            self.log.append(content)
            print('STATE log['+str(len(self.log))+']=['+str(self.term)+',"'+content+'"]' )
            return

        #modify
        if len(msg) < 4:
            return
        
        srcpid=int(msg[1])
        msgtype=msg[2]
        term=int(msg[3])
        
        if self.term<term:
            self.stepdown(term)

        if msgtype=='RequestVotes':
##self.send(i,'RequestVotes',self.term,lastLogTerm,LastLogId)
            #modify
            lastLogTerm = msg[4]
            lastLogId = msg[5]
            
            agree=False
            
            if self.term==term and (self.votedFor in {None,srcpid}):
                #modify
##                cond1 = (lastLogTerm>self.logTerm(self.log,len(self.log)))
##                cond2 = (lastLogTerm==self.logTerm(self.log,len(self.log))) and (lastLogId>len(self.log))
##                if cond1 or cond2:
##                
##                    agree=True
##                    self.votedFor=srcpid
##                    self.resetTimer()
                agree=True
                self.votedFor=srcpid
                self.resetTimer()
            
            self.send(srcpid,'RequestVotesResponse',self.term,agree)

            


        if msgtype=='RequestVotesResponse':

            agree = (msg[4]=='True')
            if agree and self.state=='"CANDIDATE"' and term==self.term:
                
                self.voteGranted[srcpid]=agree
                if self.voteGranted.count(True) > self.n//2:
                    self.becomeLeader()

        if msgtype=='AppendEntries':
##            
##      self.send(i,'AppendEntries',self.term, prevId, prevTerm, entry, commitId)
            #modify
            prevId = msg[4]
            prevTerm = msg[5]
            entry = msg[6]
            commitId = msg[7]

            success=False
            matchId = 0

            if self.term==term:
                self.resetTimer()
                self.state='"FOLLOWER"'
                print(f'STATE',f'state={self.state}')
                
                if self.leader!=srcpid:
                    self.leader=srcpid
                    print(f'STATE',f'leader={self.leader}')
                
            #modify3
                cond1 = (prevId==0)
                cond2 = (prevId<=len(self.log) and (self.logTerm(self.log, prevId)==prevTerm))
                if cond1 or cond2:
                    success=True
                    ind = prevId
                    for i in range(len(entry)):
                        ind+=1
                        if self.logTerm(self.log, ind) != entry[i].term:
                            while len(self.log) >= ind:
                                self.log = self.log[:-1]
                            self.log.push(entry[i])
                    matchId = ind
                    self.commitId = max(self.commitId, commitId)
                
            
            self.send(srcpid,'AppendEntriesResponse',self.term,success, matchId)

        if msgtype=='AppendEntriesResponse':
##  self.send(srcpid,'AppendEntriesResponse',self.term,success, matchId)
            if self.state!='"LEADER"':
                return
            if self.term != term:
                return
            agree = (msg[4]=='True')
            matchId = msg[5]
            if agree:
                self.matchId[srcpid] = max(self.matchId[srcpid], matchId)
                self.nextId[srcpid] = matchId+1
            else:
                self.nextId[srcpid] = max(1, self.nextId[srcpid]-1)
                
   
    
    


    def msgHandler(self):

        while msg:=sys.stdin.readline():
            l.acquire()
            self.processmsg(msg)
            l.release()



    def heartbeatThread(self,term):
        l.acquire()
        #modify1, LEADER
        #modify3, heartbeat time reset
        while self.term==term and self.state=='"LEADER"':
            for i in range(self.n):
                if i!=self.pid:
                    #modify3, 
                    #self.send(i,'AppendEntries',self.term)
                    if self.nextId[i] > len(self.log):
                        continue
                    prevId = self.nextId[i] - 1
                    lastId = len(self.log)
                    if self.matchId[i] <= self.nextId[i]:
                        lastId = prevId
                    prevTerm = self.logTerm(self.log, prevId)
                    entry = self.log[prevId:lastId]
                    commitId = min(self.commitId, lastId)
                    self.send(i,'AppendEntries',self.term, prevId, prevTerm, entry, commitId)
            l.release()
            time.sleep(self.ELECTION_TIMEOUT/4)
            l.acquire()
        l.release()

    def timeoutHandlerThread(self):
        l.acquire()
        if self.timer==threading.current_thread():
            self.resetTimer()
            self.term+=1
            print(f'STATE',f'term={self.term}')

            self.votedFor=self.pid
            self.state='"CANDIDATE"'
            print(f'STATE',f'state={self.state}')

            self.voteGranted=[False]*self.n
            self.voteGranted[self.pid]=True
            for i in range(self.n):
                if i!=self.pid:
                    #modify3
                    lastLogTerm = self.logTerm(self.log, len(self.log))
                    lastLogId = len(self.log)
                    self.send(i,'RequestVotes',self.term,lastLogTerm,lastLogId)
##                    self.send(i,'RequestVotes',self.term)

        l.release()



r=Raft(n,pid)

r.msgHandler()



        


        

