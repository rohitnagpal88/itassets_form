import asyncio
import signal
from nats.aio.client import Client as NATS
from stan.aio.client import Client as STAN
from cassandra.cluster import Cluster
from cassandra.query import dict_factory
import redis


cluster = Cluster()
session = cluster.connect('testjunglee')
session.row_factory = dict_factory
asset_lookup_stmt = session.prepare("SELECT json * FROM asset WHERE asset_owner_emailid=?")

r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)


#loop = asyncio.get_event_loop()
#nc = NATS()
#sc = STAN()

#async def initconnect(loop):
#    await nc.connect(io_loop=loop)  
#    await sc.connect("test-cluster", "client-sub", nats=nc)


#loop.run_until_complete(initconnect(loop))

#async def closeconnect(loop):
#    await sc.close()
#    await nc.close()



def signalHandler(signum, frame):
    print("Ctrl-C pressed..closing connection to cassandra before shutting down..")
    cluster.shutdown()
    #loop.run_until_complete(closeconnect(loop))
    #loop.close()
    sys.exit(0)



#async def run(loop):
def run():
    # Use borrowed connection for NATS then mount NATS Streaming
    # client on top.
    nc = NATS()
    nc.connect()

    # Start session with NATS Streaming cluster.
    sc = STAN()
    sc.connect("test-cluster", "client-123", nats=nc)
    
    total_messages = 0
    #future = asyncio.Future(loop=loop)
    def queue_cb(msg):
        #nonlocal future
        nonlocal total_messages
        print("Received a message (seq={}): {}".format(msg.seq, msg.data.decode()))
        total_messages += 1
        print(total_messages)
        r = binary_to_dict(msg.data.decode())
        print(r)
    #    if total_messages >= 2:
    #        future.set_result(None)

    # Subscribe to get all messages since beginning.
    sub = sc.subscribe("hi", queue="bar", cb=queue_cb)
    #await asyncio.wait_for(future, 10, loop=loop)



def binary_to_dict(the_binary):
    jsn = ''.join(chr(int(x, 2)) for x in the_binary.split())
    d = json.loads(jsn)  
    return d


if __name__ == '__main__':
    signal.signal(signal.SIGINT,signalHandler);
    #loop = asyncio.get_event_loop()
    #loop.run_until_complete(run(loop))
    #loop.run_forever()
    run()