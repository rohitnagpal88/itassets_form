import os
import sys
os.environ['PYTHONASYNCIODEBUG'] = '1'
import asyncio
import asgiref
import json
import concurrent.futures
from nats.aio.client import Client as NATS
from stan.aio.client import Client as STAN
from cassandra.cluster import Cluster
from cassandra.query import dict_factory
import uuid
import signal
import datetime
import redis

cassandra_hosts = os.environ['CASSANDRA_HOSTS']
cassandra_port = os.environ['CASSANDRA_PORT']
cassandra_keyspace = os.environ['CASSANDRA_KEYSPACE']
redis_host = os.environ['REDIS_HOST']
redis_port = os.environ['REDIS_PORT']
nats_hostport = os.environ['NATS_HOSTPORT']
clientid = os.environ['NATS_CLIENTID']

#if len (sys.argv) != 2 :
#    print("Usage: python sub.py <clientid>")
#    sys.exit (1)

#clientid = sys.argv[1]

cluster = Cluster(cassandra_hosts,port=cassandra_port)
session = cluster.connect(cassandra_keyspace)
session.row_factory = dict_factory
asset_lookup_stmt = session.prepare("SELECT json * FROM asset WHERE asset_owner_emailid=?")

r = redis.Redis(host=redis_host, port=redis_port, db=0, decode_responses=True)

loop = asyncio.get_event_loop()
executor = concurrent.futures.ThreadPoolExecutor(max_workers=3)


def signalHandler(signum, frame):
    print("Ctrl-C pressed..closing connection to cassandra before shutting down..")
    cluster.shutdown()
    #loop.run_until_complete(closeconnect(loop))
    #loop.close()
    sys.exit(0)


async def disconnected_cb():
    print("Got disconnected!")

async def reconnected_cb():
    # See who we are connected to on reconnect.
    print("Got reconnected to {url}".format(url=nc.connected_url.netloc))

async def run(loop):
    nc1 = NATS()
    sc1 = STAN()
    await nc1.connect(nats_hostport,io_loop=loop,max_reconnect_attempts=10,reconnected_cb=reconnected_cb,disconnected_cb=disconnected_cb,)
    await sc1.connect("test-cluster", clientid, nats=nc1)

    #nc2 = NATS()
    #sc2 = STAN()
    #await nc2.connect(io_loop=loop,max_reconnect_attempts=10,reconnected_cb=reconnected_cb,disconnected_cb=disconnected_cb,)
    #await sc2.connect("test-cluster", "client-sub12", nats=nc2)

    #nc3 = NATS()
    #sc3 = STAN()
    #await nc3.connect(io_loop=loop,max_reconnect_attempts=10,reconnected_cb=reconnected_cb,disconnected_cb=disconnected_cb,)
    #await sc3.connect("test-cluster", "client-sub12", nats=nc3)

    group = [sc1]

    for sc in group:
        async def queue_cb(msg):
            nonlocal sc
            print("hello")
            #print("[{}] Received a message on queue subscription: {}".format(msg.sequence, msg.data))
            await insertasset(msg)
            print("hello again there!")

        #async def regular_cb(msg):
        #    nonlocal sc
        #    print("[{}] Received a message on a regular subscription: {}".format(msg.sequence, msg.data))

        await sc.subscribe("hi", queue="workers", durable_name="durable", cb=queue_cb)
        #await sc.subscribe("foo", cb=regular_cb)

    #for i in range(0, 10):
    #    await sc.publish("foo", 'hello-{}'.format(i).encode())

    #await asyncio.sleep(1, loop=loop)

    # Close NATS Streaming session
    #for sc in group:
    #    await sc.close()
    #await nc1.close()
    #await nc2.close()
    #await nc3.close()


async def insertasset(msg):
    print("inside insertasset:")
    #await sync_to_async(testfunc)(msg)
    #print("[{}] Received a message on queue subscription: {}".format(msg.sequence, json.loads(msg.data.decode('utf-8'))))
    await loop.run_in_executor(executor, testfunc, msg)
    #print(json.loads(msg.data.decode('utf-8')))

def testfunc(msg):
    result = json.loads(msg.data.decode('utf-8'))    
    print(result)
    print(type(result))
    assetid = uuid.uuid4()
    session.execute(
        """
        INSERT INTO asset (asset_id,asset_cat,asset_condition,asset_cost,asset_desc,asset_loaned_date,asset_model,asset_owner_emailid,asset_purchase_date,asset_serial,asset_warrantyexp_date)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",(assetid,result['assetCategory'],result['condition'],float(result['assetCost']),result['assetDescription'],datetime.datetime.strptime(result['dateLoaned'], "%m/%d/%Y").date().strftime("%Y-%m-%d"),result['modelNo'],result['assetOwnerEmailId'],datetime.datetime.strptime(result['datePurchased'], "%m/%d/%Y").date().strftime("%Y-%m-%d"),result['serialNo'],datetime.datetime.strptime(result['dateWarrantyExp'], "%m/%d/%Y").date().strftime("%Y-%m-%d")))
    if r.exists(result['assetOwnerEmailId']):
        updatecache(result['assetOwnerEmailId'],empassets=[])


def updatecache(emp,empassets):
    if not empassets:
        result = session.execute(asset_lookup_stmt,(emp,))
        for results in result:
            empassets.append(results)
        empassets = json.dumps(empassets,indent=4,sort_keys=True)
    r.set(emp,empassets)


if __name__ == '__main__':
    signal.signal(signal.SIGINT,signalHandler);
    loop.run_until_complete(run(loop))
    loop.run_forever()
    loop.close()
