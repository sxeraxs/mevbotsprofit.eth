import web3
import json
from .erc20 import abi
from .eth import token_list
from loguru import logger as log


class Tokens(object):
    __erc20_abi = None
    __eth_token_list = None
    __w3: web3.AsyncWeb3 = None

    def __init__(self, w3: web3.AsyncWeb3):
        self.__w3 = w3
        self.__erc20_abi = json.loads(abi)
        self.__eth_token_list = json.loads(token_list)

    async def get_balances(self, address):
        log.info('1');
        balances = {}
        i = 0
        for token in self.__eth_token_list:
            contract = self.__w3.eth.contract(token['address'], abi=self.__erc20_abi)
            balance = await contract.functions.balanceOf(address).call()
            balances[token['symbol']] = balance

            log.info(f"{i} {token['symbol']} {balance}")
            i = i + 1
            if i == 100:
                break

        log.info('2')
        return balances

    async def get_balance(self, address, symbol):
        for token in self.__eth_token_list:
            if token['symbol'] == symbol:
                contract = await self.__w3.eth.contract(token['address'], abi=self.__erc20_abi)
                return await contract.functions.balanceOf(address).call()

        return None
