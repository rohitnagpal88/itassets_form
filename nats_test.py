import asyncio
from nats.aio.client import Client as NATS
from stan.aio.client import Client as STAN

async def run(loop):
    # Use borrowed connection for NATS then mount NATS Streaming
    # client on top.
    nc = NATS()
    await nc.connect(io_loop=loop)

    # Start session with NATS Streaming cluster.
    sc = STAN()
    await sc.connect("test-cluster", "client-123", nats=nc)

    await sc.publish("hi", b'hello')

    #await asyncio.sleep(1, loop=loop)

    await sc.close()
    await nc.close()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run(loop))
    loop.close()