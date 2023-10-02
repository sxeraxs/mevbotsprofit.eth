#!/usr/bin/env python3
import time
import config
from collector import Collector
from loguru import logger as log
from web3.providers import WebsocketProviderV2


def main():
    ws = WebsocketProviderV2(config.ETH_PROVIDER_URL)

    log.info(f"starting collector eth provider url {config.ETH_PROVIDER_URL}")
    collector = Collector(ws, config.ETH_MEV_ADDRESSES, config.ETHERSCAN_API_KEY)
    collector.start()

    try:
        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        collector.stop()


if __name__ == '__main__':
    main()
