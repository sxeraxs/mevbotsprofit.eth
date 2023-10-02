import asyncio
import json

from web3 import Web3
from web3 import AsyncWeb3
from threading import Thread
from .tokens import Tokens
from loguru import logger as log
from web3.providers import WebsocketProviderV2


class Collector(object):
    __ws: WebsocketProviderV2 = None
    __running: bool = False
    __latest_block_hash: str = ""
    __threads: []
    __addresses: []
    __etherscan_api_key: str = ""

    def __init__(self, ws, addresses, etherscan_api_key):
        self.__ws = ws
        self.__running = False
        self.__addresses = addresses
        self.__latest_block_hash = ""
        self.__threads = []
        self.__web3 = None
        self.__etherscan_api_key = etherscan_api_key

    def start(self):
        self.__running = True
        self.__threads.append(Thread(target=self.__run_eth_subscription))
        for thread in self.__threads:
            thread.start()

    def stop(self):
        self.__running = False
        for thread in self.__threads:
            thread.join()

    def __run_eth_subscription(self):
        asyncio.run(self.__new_block_subscription())

    async def __on_new_block(self, w3, response):
        tokens = Tokens(w3)
        new_block_hash = response['result']['hash'].hex()
        log.info(f"received new block {new_block_hash}")
        for address in self.__addresses:
            balances = await tokens.get_balances(address)
            log.info(json.dumps(balances, indent=4))

        self.__latest_block_hash = new_block_hash

    async def __new_block_subscription(self):
        async with AsyncWeb3.persistent_websocket(self.__ws) as w3:
            subscription_id = await w3.eth.subscribe("newHeads")
            block = await w3.eth.get_block('latest')
            self.__latest_block_hash = block.hash.hex()
            log.info(f"starting at block {self.__latest_block_hash}")

            subscribed = True
            while subscribed:
                async for response in w3.ws.listen_to_websocket():
                    await self.__on_new_block(w3, response)

                    if not self.__running:
                        subscribed = not await w3.eth.unsubscribe(subscription_id)
                        break
