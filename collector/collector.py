import redis
import asyncio
from web3 import AsyncWeb3
from threading import Thread
from loguru import logger as log
from web3.providers import WebsocketProviderV2


class Collector(object):
    __ws: WebsocketProviderV2 = None
    __db: redis.Redis = None
    __running: bool = False
    __latest_block_hash: str = ""
    __threads: []
    __contracts: []

    def __init__(self, db, ws):
        self.__ws = ws
        self.__db = db
        self.__running = False
        self.__latest_block_hash = ""
        self.__threads = []
        self.__contracts = []
        self.__web3 = None

    def start(self):
        self.__running = True
        self.__threads.append(Thread(target=self.__run_eth_subscription))
        self.__threads.append(Thread(target=self.__run_db_subscription))
        for thread in self.__threads:
            thread.start()

    def stop(self):
        self.__running = False
        for thread in self.__threads:
            thread.join()

    def __run_eth_subscription(self):
        asyncio.run(self.__new_block_subscription())

    def __run_db_subscription(self):
        asyncio.run(self.__new_contract_subscription())

    async def __on_new_block(self, w3, response):
        new_block_hash = response['result']['hash'].hex()
        log.info(f"received new block {self.__latest_block_hash}")
        for contract in self.__contracts:
            old_balance = await w3.eth.get_balance(contract, self.__latest_block_hash)
            new_balance = await w3.eth.get_balance(contract, new_block_hash)
            profit = new_balance - old_balance
            if profit != 0:
                log.info(f"set profit {contract} {new_block_hash} {profit}")
                self.__db.hset(contract, new_block_hash, f"{new_balance}#{profit}")

        self.__latest_block_hash = new_block_hash

    def __on_new_contract(self, contract):
        log.info(f"received new contract {contract}")
        self.__contracts.append(contract)
        self.__db.hset(contract, self.__latest_block_hash, "-#-")

    async def __new_contract_subscription(self):
        sub = self.__db.pubsub()
        log.info(f"subscribing to topic /new-contract")
        sub.subscribe('/new-contract')

        for contract in sub.listen():
            if contract['data'] == 1:
                continue

            self.__on_new_contract(contract['data'])
            if not self.__running:
                break

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
