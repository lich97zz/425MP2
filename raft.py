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
        
        self.votedFor=pid
        self.term=0
        print(f'STATE',f'term={self.term}')


        self.voteGranted=[False]*n
        self.ELECTION_TIMEOUT=1

        self.timer=None
        self.resetTimer()

        self.heartbeat=None

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
        print('SEND',destpid,*args,flush=True)
    
    def becomeLeader(self):
        self.state='"LEADER"'
        print(f'STATE',f'state={self.state}')

        self.leader=self.pid
        print(f'STATE',f'leader={self.leader}')

        self.heartbeat=threading.Thread(target=self.heartbeatThread,args=(self.term,))
        self.heartbeat.start()

    def processmsg(self,msg):
        msg=msg.split()
        srcpid=int(msg[1])
        msgtype=msg[2]
        
        term=int(msg[3])
        
        if self.term<term:
            self.stepdown(term)

        if msgtype=='RequestVotes':
            
            agree=False
            

            if self.term==term and (self.votedFor in {None,srcpid}):
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
            
            success=False

            if self.term==term:
                self.state='"FOLLOWER"'
                print(f'STATE',f'state={self.state}')
                
                if self.leader!=srcpid:
                    self.leader=srcpid
                    print(f'STATE',f'leader={self.leader}')
                
                

                success=True
                self.resetTimer()
            
            self.send(srcpid,'AppendEntriesResponse',self.term,success)

        if msgtype=='AppendEntriesResponse':
            pass
    
    


    def msgHandler(self):

        while msg:=sys.stdin.readline():
            l.acquire()
            self.processmsg(msg)
            l.release()



    def heartbeatThread(self,term):
        l.acquire()
        #modify1, LEADER
        while self.term==term and self.state=='"LEADER"':
            for i in range(self.n):
                if i!=self.pid:
                    self.send(i,'AppendEntries',self.term)
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
                    self.send(i,'RequestVotes',self.term)

        l.release()



r=Raft(n,pid)

r.msgHandler()



        


        

