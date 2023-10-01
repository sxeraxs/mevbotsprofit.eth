#!/usr/bin/env python3
import time
import redis
import config
from collector import Collector
from loguru import logger as log
from web3.providers import WebsocketProviderV2


def main():
    db = redis.Redis(host=config.DB_HOST, port=config.DB_PORT, decode_responses=True)
    ws = WebsocketProviderV2(config.ETH_PROVIDER_URL)

    log.info(f"starting collector db {config.DB_TYPE} {config.DB_HOST}:{config.DB_PORT} eth provider url {config.ETH_PROVIDER_URL}")
    collector = Collector(db, ws)
    collector.start()

    try:
        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        collector.stop()


if __name__ == '__main__':
    main()
