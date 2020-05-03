__authors__ = 'Winshen Liu and Aaron Tinn'

'''device will act as intermediary between servers for pubs and subs'''

import zmq

def main():
    try:
        context = zmq.Context(1)
        # Socket facing clients
        frontend = context.socket(zmq.SUB)
        frontend.bind("tcp://*:5011")

        frontend.setsockopt_string(zmq.SUBSCRIBE, "")

        # Socket facing servers
        backend = context.socket(zmq.PUB)
        backend.bind("tcp://*:5012")
        print('Forwarder is activated')
        zmq.device(zmq.FORWARDER, frontend, backend)

    except Exception as e:
        print(e)
        print("bringing down the broker")

    finally:

        frontend.close()
        backend.close()
        context.term()

if __name__ == "__main__":
    main()