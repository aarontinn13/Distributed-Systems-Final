__authors__ = 'Winshen Liu and Aaron Tinn'

import zmq, json, sys, time, threading, random, re, pickle, os

HOST = '127.0.0.1'
PUB_PORT = 5011                                                                         # publish port to the broker
SUB_PORT = 5012                                                                         # subscribe port from the broker
DATABASE = {}

class Server():
    def __init__(self, client_port):
        self.port = client_port                                                         # port that the server will communicate with the client
        self.responder = None                                                           # context that the server will communicate with the client
        self.publisher = None                                                           # context that the server will publish messages to other servers
        self.heartbeat_sub = None                                                       # subscription socket to listen for heartbeat topics
        self.election_sub = None                                                        # subscription socket to listen for election topics
        self.state = 'follower'                                                         # current state (starts as follower,can become leader or candidate)
        self.delta = random.uniform(1.5, 5)                                             # random value between x and y of how long a server needs to wait (need to save the value somewhere)
        self.time = time.time() + self.delta                                            # total time a server must wait until an event

        self.current_term = 0                                                           # current election term
        self.voted_for = None                                                           # who the server has voted for in the current election.
        self.leader = None                                                              # who the leader is (port of the leader)
        self.votes = set()                                                              # list of ports that voted for this server
        self.voted_already = set()                                                      # list of servers who have already voted, use this to determine when votes are finished.
        self.servers = ['5000', '5001', '5002', '5003', '5004']                         # list of servers currently alive
        self.replications = set()                                                       # set of servers that have replicated a log entry
        self.log = []                                                                   # log of what the server has saved
        self.commit_index = 0                                                           # index of when we committed to database

    def setup(self):
        '''set up the connections'''
        context = zmq.Context()                                                         # set up the context object
        self.set_responder(context)                                                     # set up communication with the client(s)
        self.set_publisher(context)                                                     # set up communication to publish to servers
        self.set_subscriber(context)                                                    # set up communication to listen to servers

    def set_responder(self, context):
        '''set the channel to receive and respond with client(s)'''
        self.responder = context.socket(zmq.REP)                                        # set up responder socket
        self.responder.bind(f"tcp://{HOST}:{self.port}")                                # bind responder

    def set_publisher(self, context):
        '''set the publish channel to send messages to other servers'''
        self.publisher = context.socket(zmq.PUB)                                        # set up publish socket
        self.publisher.connect(f"tcp://{HOST}:{PUB_PORT}")                              # connect to the forwarder to publish

    def set_subscriber(self, context):
        '''set the subscribe channel to listen to other servers'''
        self.heartbeat_sub = context.socket(zmq.SUB)                                    # set up subscribe socket for heartbeats
        self.heartbeat_sub.connect(f'tcp://{HOST}:{SUB_PORT}')                          # connect to Host and port of the forwarder
        self.heartbeat_sub.setsockopt_string(zmq.SUBSCRIBE, 'heartbeat')                # filter for heartbeat messages

        self.election_sub = context.socket(zmq.SUB)                                     # set up subscribe socket for heartbeats
        self.election_sub.connect(f'tcp://{HOST}:{SUB_PORT}')                           # connect to Host and port of the forwarder
        self.election_sub.setsockopt_string(zmq.SUBSCRIBE, 'elect')                     # filter for elect messages

    def listen_for_client(self):
        '''listen to client requests and depending on state, do something'''
        while True:
            request = self.responder.recv()                                             # receive message from client(s)
            request = json.loads(request)                                               # decode it

            if self.state == 'leader':                                                  # if server is the leader
                self.reply_to_client(request)                                           # reply to client
            elif self.state == 'follower':                                              # if server is the follower
                self.responder.send_string(str(self.leader))                            # send back port of the leader
            else:                                                                       # if server is candidate
                self.responder.send_string('Retry later.')                              # we send back retry message

    def reply_to_client(self, request):
        '''LEADER method to respond back to the client'''
        command = self.parse_command(request)                                           # get 'put' or get' from the message
        key = self.parse_key(request)                                                   # get the key from the message
        value = self.parse_value(request)                                               # get the value from the message
        entry = {'command': command, 'key': key, 'value': value}                        # prepare the entry

        message = self.create_message(entry)                                            # create the message {port, term, prevInd, prevTerm, entry, commit}
        self.append_to_log(message)                                                     # append message to own log as leader
        self.send_beat(message)                                                         # send message through heartbeat

        while len(self.replications) < len(self.servers)//2:                            # wait until more than half of followers have confirmed log entries
            pass

        self.commit_entry(command, key, value)                                          # when more than half confirm, commit the entry
        self.replications = set()                                                       # reset the replication count

    def parse_command(self, request):
        '''return either 'get' or 'put' from the message'''
        return request['type']

    def parse_key(self, request):
        '''return the key from the message'''
        return request['payload']['key']

    def parse_value(self, request):
        '''return the value from the message'''
        return request['payload']['value']

    def create_message(self, entry = None):
        '''creates message for leader to send to followers'''
        prev_log_index = self.get_prev_log_index()                                      # get the previous log index
        prev_log_term = self.get_prev_log_term(prev_log_index)                          # get the previous log term

        return {'port': self.port, 'term': self.current_term,                           # return the message
                'prev_log_index': prev_log_index,
                'prev_log_term': prev_log_term, 'entry': entry,
                'leader_commit': self.commit_index}

    def get_prev_log_index(self):
        '''LEADER method to get previous log index'''
        if len(self.log) == 0:                                                          # if the log is empty, return nothing
            return None
        else:
            return len(self.log) - 1                                                    # else return the last index

    def get_prev_log_term(self, prev_log_index):
        '''LEADER method to get previous log term from previous log index'''
        if prev_log_index is not None:
            print(f'Previous log entry is {self.log[prev_log_index]}             ')  # reveal the previous log entry
            return self.log[prev_log_index][1]                                          # get the term from this log
        else:
            return None                                                                 # if the log index is none, then no term

    def append_to_log(self, message):
        '''create a tuple to append to own log'''
        entry = (message['entry'], message['term'])                                     # create tuple with client request and term number
        self.log.append(entry)                                                          # append this entry into own log

    def commit_entry(self, command, key, value):
        '''method to confirm put into database or simply return get request'''
        print(f'COMMITTING ENTRY NOW                      ')
        if command == 'put':                                                            # if request was a put
            self.put(key, value)                                                        # update database
            self.update_commit_index()                                                  # increment update index
        else:
            self.get(key)

    def update_commit_index(self):
        '''method to update commit index'''
        self.commit_index += 1                                                          # increment update index

    def put(self, key, value):
        '''put messages into the key value store'''
        print(f'PUTTING {key}: {value}                 ')
        try:
            DATABASE[key] = value                                                       # all nodes store into database
            print(f'\nSuccessfully stored key "{key}" with value "{value}"\n                  ')
        except:
            reply = {'code': 'fail'}                                                    # if we can't update database, return with fail response
            if self.state == 'leader':
                self.responder.send_json(reply)
                print(f'\nFailed to store key "{key}" with value "{value}"\n                  ')
                return

        if self.state == 'leader':                                                      # if leader, then reply to client with success
            self.send_reply({'code': 'success'})

    def get(self, key):
        '''method for all nodes to get messages from the key value store'''
        status = 'success'

        try:
            value = DATABASE[key]
            print(f'\nSuccessfully returned value "{value}" for key "{key}"\n           ')
        except KeyError:
            value = 'not found'
            status = 'failure'
            print(f'\nFailed to return value for key "{key}"\n           ')

        if self.state == 'leader':
            self.send_reply({'code': status, 'payload': {'message': 'optional', 'key': key, 'value': value}})

    def send_reply(self, reply):
        '''LEADERS method to send reply back to the client'''
        self.responder.send_json(reply)

    def send_beat(self, entry):
        '''leader's method only to send latest log entry to followers after client request and commit once replicated'''
        self.publisher.send_multipart([b'heartbeat', pickle.dumps(entry)])

    def refresh_time(self):
        '''refresh the wait time of the server'''
        self.time = time.time() + self.delta

    def countdown(self):
        '''timer thread to countdown for events'''
        while time.time() < self.time:                                                           # timer still active
            print(f'server {self.port} time remaining: ', end='')
            print(round(self.time - time.time(), 4), end='\r')

        if len(self.servers) == 3:                                                               # if we have 2 servers remaining, kill the servers
            print('Less than N/2 servers remain, so the system is unusable. System is shutting down. Goodbye!')
            os._exit(1)

        if self.state == 'follower':                                                             # timer runs out
            self.voted_for = None                                                                # reset the voted for
            self.leader = None                                                                   # reset leader
            print(f'Server {self.port} is a candidate and is sending out votes                  ')
            self.become_candidate()                                                              # if timer runs out, become a candidate and ask for votes

    def listen_heartbeat(self):
        '''listen for 'heartbeat' topics'''
        while True:
            [topic, message] = self.heartbeat_sub.recv_multipart()
            topic = topic.decode()
            message = pickle.loads(message)
            port = message['port']
            term = int(message['term'])
            entry = message['entry']
            leader_commit = message['leader_commit']

            if self.state == 'follower':                                                        # if follower receives
                if not self.leader:                                                             # if there is no leader (first heart beat this term)
                    self.leader = port                                                          # set the leader
                    print(f'Current leader is {self.leader}             ')

                if entry is not None:                                                           # log message, not heartbeat
                    self.append_to_log(message)                                                 # update log
                    print(f'THIS IS MY LOG {self.log}                  ')
                    self.send_consensus()
                elif leader_commit > self.commit_index:                                         # heartbeat after leader has made commit
                    entry = self.log[self.commit_index][0]
                    if entry['command'] == 'put':
                        self.commit_entry(entry['command'], entry['key'], entry['value'])
                self.refresh_time()

            elif self.state == 'leader':                                                        # if leader receives a heartbeat, leader receives consensus response from followers
                if port != self.port:
                    print(f'Adding {port} to replication log                 ')
                    self.replications.add(port)

            elif self.state == 'candidate':                                                     # if a candidate receives a heartbeat, then another candidate has won, concede
                self.state = 'follower'                                                         # set back to follower
                self.voted_for = port                                                           # vote for leader
                self.votes = set()                                                              # reset votes
                self.voted_already = set()                                                      # reset voted_already
                self.leader = port                                                              # set the leader as port
                if entry is not None:                                                           # log message, not heartbeat
                    self.append_to_log(message)                                                 # update log
                    print(f'THIS IS MY LOG {self.log}                  ')
                    self.send_consensus()
                elif leader_commit > self.commit_index:                                         # heartbeat after leader has made commit
                    entry = self.log[self.commit_index][0]
                    self.commit_entry(entry['command'], entry['key'], entry['value'])
                self.refresh_time()
                restart = threading.Thread(target=self.countdown)                               # restart the timer thread
                restart.start()

    def send_consensus(self):
        '''method to return a response of the servers log to the leader'''
        self.send_beat(self.create_message())                                                   # consensus response to leader

    def listen_election(self):
        '''listen for 'elect' topics'''
        while True:
            [topic, message] = self.election_sub.recv_multipart()
            topic = topic.decode()
            message = pickle.loads(message)
            port = message['port']
            term = int(message['term'])
            cand_log_index = message['prev_log_index']
            cand_log_term = message['prev_log_term']
            voted_for = message['voted_for']

            if self.state == 'follower':                                                            # receive vote request from candidate

                if self.current_term < term:                                                        # this indicates we are starting a new round of vote request
                    self.voted_for = None                                                           # reset voted_for

                if self.leader:                                                                     # this indicates the leader has died.
                    self.servers.remove(self.leader)                                                # remove the leader from the active servers
                    self.voted_for = None                                                           # reset voted_for
                    self.leader = None                                                              # reset leader

                if self.voted_for is None and cand_log_index == self.get_prev_log_index():                                  # the candidate has already voted for itself and is requesting votes from other followers
                    print(f'Log indices match between candidate {cand_log_index} and self {self.get_prev_log_index()}')
                    self.voted_for = port                                                           # set the current leader to this port
                    self.refresh_time()                                                             # refresh the time
                    self.current_term += 1                                                          # increase current term
                    self.send_vote(port)                                                            # vote for the candidate by sending candidate's port number

            elif self.state == 'candidate':
                if port != self.port:                                                               # if the port is not own port
                    if voted_for == self.port:                                                      # if the sender's votedfor matches your own port, this server voted for you
                        self.votes.add(port)                                                        # add this port to the list of votes that voted for you.
                    self.voted_already.add(port)                                                    # add this port to the set of servers who have already completed votes

    def become_candidate(self):
        '''when server times out, become candidate and run the candidate methods'''
        if self.leader:                                                                             # this indicates the leader has died
            self.servers.remove(self.leader)                                                        # remove leader from the server list
            self.leader = None                                                                      # reset the leader to None
        self.state = 'candidate'                                                                    # switch state to candidate
        self.current_term += 1                                                                      # increase the current term
        self.voted_for = self.port                                                                  # vote for self
        self.votes.add(self.port)                                                                   # add yourself to your vote list of people who have voted for you
        self.voted_already.add(self.port)                                                           # add yourself to people who have voted
        self.request_vote()                                                                         # send a vote request immediately to other servers

        while self.state != 'leader' or not self.leader:                                            # while there is no leader
            if len(self.votes) > len(self.servers)//2:                                              # immediately when a candidate receives more than half the total server votes, they are the leader!
                self.leader = self.port                                                             # set the leader to own port
                self.become_leader()                                                                # become the leader
                break

            elif len(self.voted_already) == len(self.servers):                                      # when everyone has finished voting and no leader has been elected, there is a split vote
                if len(self.votes) < len(self.servers)//2:
                    print('RE-ELECTION PHASE!                          ')
                    self.current_term += 1                                                              # go to the next term
                    self.votes = {self.port}                                                            # reset votes to self only again
                    self.voted_already = {self.port}                                                    # reset who voted this term ot just self
                    time.sleep(random.uniform(.25, .75))                                                 # sleep a random time that must be less than the minimum of the normal delta range
                    self.request_vote()                                                                 # request for votes again

    def send_election(self, entry):
        '''leader's method only to send latest log entry to followers after client request and commit once replicated'''
        self.publisher.send_multipart([b'elect', pickle.dumps(entry)])

    def request_vote(self):
        '''upon becoming candidate, send out vote request'''
        self.send_election(self.create_vote())
        print('Requesting votes from other servers...                ')

    def send_vote(self, candidate):
        '''upon receiving vote from candidate, send back own port'''
        self.send_election(self.create_vote())
        print(f'Published vote for Server: {candidate}                   ')

    def create_vote(self):
        '''creates message for candidate to send to followers'''
        prev_log_index = self.get_prev_log_index()                                      # get the previous log index
        prev_log_term = self.get_prev_log_term(prev_log_index)                          # get the previous log term

        return {'port': self.port, 'term': self.current_term,                           # return the message
                'prev_log_index': prev_log_index,
                'prev_log_term': prev_log_term, 'voted_for': self.voted_for}

    def become_leader(self):
        self.state = 'leader'
        print(f'Server {self.port} is now the leader                 ')
        while True:
            time.sleep(1)
            self.send_beat(self.create_message())
            print('sending heartbeat...                          ')

    def run(self):
        '''main driver to run'''                                           # set up the contexts and sockets
        t1 = threading.Thread(target=self.listen_for_client)               # start a thread for listening to clients
        t2 = threading.Thread(target=self.listen_election)                 # start a thread for election topics
        t3 = threading.Thread(target=self.listen_heartbeat)                # start a thread for heartbeat topics
        t4 = threading.Thread(target=self.countdown)                       # start a thread for the clock

        t1.start()
        t2.start()
        t3.start()
        t4.start()

def main():
    '''main control'''
    server = Server(sys.argv[1])                                                        # pass in user input port
    print(f'Server {sys.argv[1]} is starting...')
    server.setup()
    server.run()

if __name__ == '__main__':
    main()
