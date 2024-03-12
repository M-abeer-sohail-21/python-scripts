import asyncio
from aiocoap import Context, Message

async def main():
    protocol = await Context.create_client_context()

    request = Message(code=1, uri='coap://k8s.hub.xelerate.solutions:56385/012345678900000/012345678900000/012345678900000') # Added port 56385 to the URI
    response = await protocol.request(request).response

    print('Result: %s\n%r'%(response.code, response.payload))

if __name__ == "__main__":
    asyncio.run(main())
