import asyncio
#import asgiref
from nats.aio.client import Client as NATS
from stan.aio.client import Client as STAN


#msgs = []

async def run(loop):
    nc1 = NATS()
    sc1 = STAN()
    await nc1.connect(io_loop=loop)
    await sc1.connect("test-cluster", "client-sub7", nats=nc1)

    #group = [sc1, sc2, sc3]

    #future = asyncio.Future()
    async def queue_cb(msg):
        nonlocal sc1
        #nonlocal future
        #future.set_result(msg)
        print("[{}] Received a message on queue subscription: {}".format(msg.sequence, msg.data))
        #r = json.loads(msg.data.decode('utf-8'))    
        #print(r)
        #print(type(r))

    await sc1.subscribe("hi",queue="workers", cb=queue_cb)

    #msg = await asyncio.ensure_future(future)
    #print(msg.data)
    #result.set_result(msg)
    #for i in range(0, 10):
    #    await sc.publish("foo", 'hello-{}'.format(i).encode())

    #await asyncio.sleep(1, loop=loop)

    # Close NATS Streaming session
    
    #for sc in group:
    await sc1.close()
    await nc1.close()
    #await nc2.close()
    #await nc3.close()

#def insertasset(msg):
#    print("[{}] Received a message on queue subscription: {}".format(msg.sequence, msg.data))
#    return


#result = asyncio.ensure_future(run(loop),loop=loop)

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run(loop))
    #loop.run_until_complete(result)
    #print(result)
    #loop.run_forever()
    #print(result)
    #loop.run_forever()
    loop.close()
