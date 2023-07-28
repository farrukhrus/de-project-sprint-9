from datetime import datetime
from logging import Logger

from lib.kafka_connect import KafkaConsumer
from cdm_loader.repository.cdm_repository import USERS, CATEGORIES
from cdm_loader.repository.cdm_repository import LOAD


class CdmMessageProcessor:
    def __init__(
        self,
        consumer: KafkaConsumer,
        stg_repository: LOAD,
        logger: Logger,
    ) -> None:
        self._logger = logger
        self._batch_size = 100
        self._consumer = consumer
        self._stg_repository = stg_repository

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")

        for _ in range(self._batch_size):
            msg = self._consumer.consume()
            if not msg:
                break

            self._logger.info(f"{datetime.utcnow()}: Message received")

            payload = msg["payload"]
            user_id = payload["user_id"]
            products = [
                USERS(
                    user_id=user_id,
                    product_id=item["id"],
                    product_name=item["name"],
                    order_cnt=item["orders_cnt"],
                )
                for item in payload["product"]
            ]
            categories = [
                CATEGORIES(
                    user_id=user_id,
                    category_id=item["id"],
                    category_name=item["name"],
                    order_cnt=item["orders_cnt"],
                )
                for item in payload["category"]
            ]

            for product in products:
                self._stg_repository.insert(product)

            for category in categories:
                self._stg_repository.insert(category)

            self._logger.info(f"{datetime.utcnow()}. Insert to CDM successful")

        self._logger.info(f"{datetime.utcnow()}: FINISH")
