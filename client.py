__authors__ = 'Winshen Liu and Aaron Tinn'

'''client'''

import zmq, json, sys


HOST = '127.0.0.1'

class Client():
    def __init__(self):
        self.port = None
        self.servers = ['5000', '5001', '5002', '5003', '5004']
        self.requester = None

    def request_port(self):
        '''input port to connect to'''
        while self.port is None:                                                # while there is no port
            port = input(f'Enter a port to connect to {self.servers}: ')        # get input

            if port in self.servers:                                            # if port is valid
                self.port = port                                                # set the port
            else:                                                               # else
                print('Sorry, that is not a valid port. Please try again.')     # ask again

        self.setup()                                                            # set up connection to the server
        self.request_message()                                                  # request input to send message to server

    def setup(self):
        '''set up the connection to the server'''
        context = zmq.Context()                                                 # set up the context
        self.requester = context.socket(zmq.REQ)                                # set up the socket to the server
        self.requester.setsockopt(zmq.LINGER, 0)                                # set up option to hang
        self.requester.RCVTIMEO = 1000                                          # keep message in queue for one second to break
        self.requester.connect("tcp://{}:{}".format(HOST, self.port))           # connect to the port
        print(f'Connected to port {self.port}')

    def request_message(self):
        '''request input to send message to the server'''
        while True:

            operation = input('Enter \"put\" or \"get\": ').rstrip().lower()    # enter a put or get
            self.do_command(operation)

    def do_command(self, operation):
        '''parse the command'''
        if operation == 'put':                                                  # if command is put
            self.put()                                                          # prepare put request
            print(f'\nSuccessfully sent "{operation}" request\n')
        elif operation == 'get':                                                # if command is get
            self.get()                                                          # prepare get request
            print(f'\nSuccessfully sent "{operation}" request\n')
        else:                                                                   # if command is neither
            print('\nPlease enter either "put" or "get".')                      # retry

    def put(self):
        '''prepares and sends put request'''
        key = input('Enter a key to store (e.g. "Name"): ')                     # get input for key
        value = input('Enter a value to store (e.g. "Aaron"): ')                # get input for value
        store = {'type': 'put', 'payload': {'key': key, 'value': value}}        # prepare the message
        try:
            self.send_request(store)                                            # attempt to send message
        except zmq.error.Again:                                                 # server is dead
            self.handle_dead_server()                                           # handle dead server

    def get(self):
        '''prepares and sends get request'''
        key = input('Enter a key to search for (e.g. "Name"): ').rstrip()       # get input for key
        store = {'type': 'get', 'payload': {'key': key, 'value': None}}         # prepare the message
        try:
            self.send_request(store)                                            # attempt to send message
        except zmq.error.Again:                                                 # server is dead
            self.handle_dead_server()                                           # handle dead server

    def handle_dead_server(self):
        '''method to handle dead servers'''
        print('\nThat server is no longer valid. Please try a different port.')
        self.servers.remove(self.port)
        self.port = None                                                        # reset the server to None
        self.request_port()                                                     # ask to input another server

    def send_request(self, store):
        '''method to send send request'''
        self.requester.send_json(store)                                         # send the request to the server
        message = self.requester.recv()                                         # receive response from server

        if message.decode() in ['5000', '5001', '5002', '5003', '5004']:        # if message is a port, we connected to follower
            print(f'Redirecting to {message.decode()} and resending request.')
            self.port = message.decode()                                        # re-connect to this port
            self.redirect_request(store)                                        # setup the connection and send message again
        elif message.decode() == 'Retry later':                                 # servers currently in election
            print('Server timeout due to election. Please try again later.')
            sys.exit()                                                          # kill the client (try again)
        else:                                                                   # connected to leader
            print(json.loads(message))                                          # print the response from the leader

    def redirect_request(self, store):
        '''method to re-setup connection and send message'''
        self.setup()                                                            # setup connection again
        self.send_request(store)                                                # send message again

def main():
    client = Client()                                                           # set up the client
    client.request_port()                                                       # start the client

if __name__ == '__main__':
    main()
