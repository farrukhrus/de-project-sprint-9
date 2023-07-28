from dataclasses import dataclass
from typing import List
from uuid import UUID

from lib.pg import PgConnect


@dataclass
class USERS:
    user_id: UUID
    product_id: UUID
    product_name: str
    order_cnt: int

    def table_name(self) -> str:
        return "cdm.user_product_counters"

    def unique_column_names(self) -> List[str]:
        return ["user_id", "product_id"]


@dataclass
class CATEGORIES:
    user_id: UUID
    category_id: UUID
    category_name: str
    order_cnt: int

    def table_name(self) -> str:
        return "cdm.user_category_counters"

    def unique_column_names(self) -> List[str]:
        return ["user_id", "category_id"]


class LOAD:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def insert(self, obj) -> None:
        table_name = obj.table_name()
        unique_column_names = obj.unique_column_names()
        column_names = list(obj.__fields__.keys())
        column_names_str = ", ".join(column_names)
        unique_column_names_str = ", ".join(unique_column_names)
        values_names_str = ", ".join([f"%({name})s" for name in column_names])
        update_names_str = ", ".join(
            [f"{name}=excluded.{name}" for name in column_names]
        )

        query = f"""
        insert into {table_name} ({column_names_str})
        values ({values_names_str})
        on conflict ({unique_column_names_str})
        do update set {update_names_str};
        """

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, obj.dict())
