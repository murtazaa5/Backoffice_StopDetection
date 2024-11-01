import asyncio
from StartProcessing import startReceivingGPS

async def main():

    await startReceivingGPS()  

if __name__ == "__main__":
    asyncio.run(main())