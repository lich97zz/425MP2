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
        self.logcontent=[]
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
        return log[ind-1]

    def resetTimer(self):
        
        #fix concurrency issue
        
        self.timer=threading.Timer(self.ELECTION_TIMEOUT*(1+random()),self.timeoutHandlerThread)
        self.timer.start()


    def stepdown(self,term):
        self.term=term
        print(f'STATE',f'term={self.term}')

        self.state='"FOLLOWER"'
        print(f'STATE',f'state={self.state}')
        
        self.votedFor=None
        self.voteGranted=[False]*self.n
        #todo: check this in heartbeat thread
        #  if self.heartbeat:
        #     self.heartbeat.cancel()
        


        

    
    def send(self,destpid,*args):
        print('SEND',destpid,*args,flush=True)
    
    def becomeLeader(self):
        self.state='"LEADER"'
        print(f'STATE',f'state={self.state}')

        self.leader=self.pid
        print(f'STATE',f'leader={self.leader}')

        self.heartbeat=threading.Thread(target=self.heartbeatThread,args=(self.term,))
        self.heartbeat.start()

    def printinfo(self):
        infodict = dict()
        infodict['n']=self.n
        infodict['pid']=self.pid
        infodict['leader'] = self.leader
        infodict['log'] = self.log
        infodict['logcontent'] = self.logcontent
        infodict['commitId'] = self.commitId
        infodict['matchId'] = self.matchId
        infodict['nextId'] = self.nextId
        print("info:",infodict)
        
    def processmsg(self,msg):
        msg=msg.split()
        
        if msg[0]=='LOG':
            content=msg[1]
            self.logcontent.append(content)
            self.log.append(self.term)
            print('STATE log['+str(len(self.log))+']=['+str(self.term)+',"'+content+'"]' )
        
##            self.printinfo()

            return
    
        srcpid=int(msg[1])
        msgtype=msg[2]
        
        term=int(msg[3])

        if self.term<term:
            self.stepdown(term)

        if msgtype=='RequestVotes':
            
##            print("entering requestVotes handler")
##self.send(i,'RequestVotes',self.term,lastLogTerm,LastLogId)
            #modify
##            lastLogTerm = int(msg[4])
##            lastLogId = int(msg[5])
            
            agree=False
            
            if self.term==term and (self.votedFor in {None,srcpid}):
                #modify
                
##                print("****info:",lastLogTerm,self.logTerm(self.log,len(self.log)))
##                cond1 = (lastLogTerm > self.logTerm(self.log,len(self.log)))
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
            
            prevId = int(msg[4])
            prevTerm = int(msg[5])
            
            tmp = msg[6][1:-1].split(',')
            entry = []
            for elm in tmp:
                if elm == '':
                    continue
                entry.append(int(elm))

            tmp = msg[7][1:-1].split(',')
            content = []
            for elm in tmp:
                if elm == '':
                    continue
                content.append(elm)
                
            commitId = int(msg[8])
            
            success=False
            matchId = 0

            if self.term==term:
                
                self.state='"FOLLOWER"'
                print(f'STATE',f'state={self.state}')
                
                if self.leader!=srcpid:
                    self.leader=srcpid
                    print(f'STATE',f'leader={self.leader}')

                self.resetTimer()
                
            #modify3
                cond1 = (prevId==0)
                cond2 = (prevId<=len(self.log) and (self.logTerm(self.log, prevId)==prevTerm))
                if cond1 or cond2:
                    success=True
                    ind = prevId
                    
                    for i in range(len(entry)):
                        ind+=1
                        if self.logTerm(self.log, ind) != entry[i]:
                            while len(self.log) > ind-1:
                                self.log = self.log[:-1]
                                self.logcontent = self.logcontent[:-1]
                            print("****info, logpushing:",entry[i],' ',content[i],' at ',self.pid)
                            self.log.append(entry[i])
                            self.logcontent.append(content[i])
                    matchId = ind
            
            self.send(srcpid,'AppendEntriesResponse',self.term,success, matchId)
            oldCommitId = self.commitId
            self.commitId = max(self.commitId, commitId)
            if self.commitId > oldCommitId:
                print("loginfo:",self.log,self.logcontent,self.commitId)
                print('STATE commitIndex='+str(self.commitId))
                for i in range(oldCommitId+1, self.commitId+1):
                    if i > len(self.log):
                        break
##                    print('COMMITTED '+str(self.logcontent[i-1])+' '+str(i))
                    print('COMMITTED '+str(self.logcontent[i-1])+' '+str(0))

        if msgtype=='AppendEntriesResponse':
            def updateCommit():
                matchIdTmp = list(self.matchId.values())
                matchIdTmp[self.pid] = len(self.log)
##                print(matchIdTmp)
                matchIdTmp.sort()
                ind = matchIdTmp[int(self.n/2)]
##                print('------Trying to update...')
##                print(matchIdTmp)
##                print(ind)
##                print(self.logTerm(self.log,ind), self.term)
                if self.state=='"LEADER"' and self.logTerm(self.log,ind)==self.term:
##                    print("*********************Entered")
                    oldCommitId = self.commitId
                    self.commitId = max(self.commitId, ind)
##                    print(oldCommitId,self.commitId)
                    if self.commitId > oldCommitId:
                        print('STATE commitIndex='+str(self.commitId))
                        for i in range(oldCommitId+1, self.commitId+1):
                            if i > len(self.log):
                                break
                            
                            print('COMMITTED '+str(self.logcontent[i-1])+' '+str(i))
                
##  self.send(srcpid,'AppendEntriesResponse',self.term,success, matchId)
            if self.state!='"LEADER"':
                return
            if self.term != term:
                return
            agree = (msg[4]=='True')
            matchId = int(msg[5])
            if agree:
                self.matchId[srcpid] = max(self.matchId[srcpid], matchId)
                self.nextId[srcpid] = matchId+1
                updateCommit()
            else:
                self.nextId[srcpid] = max(1, self.nextId[srcpid]-1)
            
    
    


    def msgHandler(self):
        while msg:=sys.stdin.readline():
            l.acquire()
            self.processmsg(msg)
            l.release()



    def heartbeatThread(self,term):
        l.acquire()
        #modify3, heartbeat time reset
        while self.term==term and self.state=='"LEADER"':
            for i in range(self.n):
                if i!=self.pid:
                    #todo
##                    if self.nextId[i] > len(self.log):
##                        continue
                    prevId = self.nextId[i] - 1
                    lastId = min(prevId+1, len(self.log))
                    if self.matchId[i] < self.nextId[i]-1:
                        lastId = prevId
                    prevTerm = self.logTerm(self.log, prevId)
                    entry = self.log[prevId:lastId]
                    content = self.logcontent[prevId:lastId]
                    commitId = min(self.commitId, lastId)
##                    print("(((((,entry=",entry)
                    self.send(i,'AppendEntries',self.term, prevId, prevTerm, entry, content, commitId)
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
##                    lastLogTerm = self.logTerm(self.log, len(self.log))
##                    lastLogId = len(self.log)
##                    print("***info1",lastLogTerm)
##                    self.send(i,'RequestVotes',self.term,lastLogTerm,lastLogId)
                    self.send(i,'RequestVotes',self.term)
        l.release()



r=Raft(n,pid)


r.msgHandler()



        


        

