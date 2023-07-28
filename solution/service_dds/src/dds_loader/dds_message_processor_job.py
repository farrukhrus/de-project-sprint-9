from datetime import datetime
from logging import Logger

from lib.kafka_connect import KafkaConsumer, KafkaProducer

from dds_loader.repository.dds_builder import BUILDER
from dds_loader.repository.dds_repository import LOAD


class DdsMessageProcessor:
    def __init__(
        self,
        consumer: KafkaConsumer,
        producer: KafkaProducer,
        dds_repository: LOAD,
        logger: Logger,
    ) -> None:
        self._consumer = consumer
        self._producer = producer
        self._dds_repository = dds_repository
        self._logger = logger
        self._batch_size = 100

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")

        for _ in range(self._batch_size):
            msg = self._consumer.consume()
            if not msg:
                break

            self._logger.info(f"{datetime.utcnow()}: Message received")

            payload = msg["payload"]
            message_in = BUILDER(payload)

            self._dds_repository.insert(message_in.h_user())
            for i in message_in.h_product():
                self._dds_repository.insert(i)
            for i in message_in.h_category():
                self._dds_repository.insert(i)
            self._dds_repository.insert(message_in.h_restaurant())
            self._dds_repository.insert(message_in.h_order())

            for i in message_in.l_order_product():
                self._dds_repository.insert(i)
            for i in message_in.l_product_restaurant():
                self._dds_repository.insert(i)
            for i in message_in.l_product_category():
                self._dds_repository.insert(i)
            self._dds_repository.insert(message_in.l_order_user())

            self._dds_repository.insert(message_in.s_user_names())
            for i in message_in.s_product_names():
                self._dds_repository.insert(i)
            self._dds_repository.insert(message_in.s_restaurant_names())
            self._dds_repository.insert(message_in.s_order_cost())
            self._dds_repository.insert(message_in.s_order_status())

            self._logger.info(f"{datetime.utcnow()}. Insert to DDS successful")

            user_id = str(message_in.h_user().h_user_pk)
            products = [
                {"id": str(obj[1]), "name": obj[2], "orders_cnt": obj[3]}
                for obj in self._dds_repository.counter(user_id, "product")
            ]
            categories = [
                {"id": str(obj[1]), "name": obj[2], "orders_cnt": obj[3]}
                for obj in self._dds_repository.counter(user_id, "category")
            ]

            message_out = {
                "object_id": user_id,
                "object_type": "user_product_category_counters",
                "payload": {
                    "user_id": user_id,
                    "product": products,
                    "category": categories,
                },
            }

            self._producer.produce(message_out)
            self._logger.info(f"{datetime.utcnow()}. Message sent")

        self._logger.info(f"{datetime.utcnow()}: FINISH")
