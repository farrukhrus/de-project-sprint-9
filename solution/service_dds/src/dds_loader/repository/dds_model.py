from dataclasses import dataclass
from datetime import datetime
from typing import List
from uuid import UUID

from pydantic import BaseModel


# базовый класс
class MODEL(BaseModel):
    def get_table_name(self) -> str:
        pass

    def get_unique_columns(self) -> List[str]:
        pass


@dataclass
class H_User(MODEL):
    h_user_pk: UUID
    user_id: str
    load_dt: datetime
    load_src: str

    def get_table_name(self) -> str:
        return "dds.h_user"

    def get_unique_columns(self) -> List[str]:
        return ["h_user_pk"]


@dataclass
class H_Product(MODEL):
    h_product_pk: UUID
    product_id: str
    load_dt: datetime
    load_src: str

    def get_table_name(self) -> str:
        return "dds.h_product"

    def get_unique_columns(self) -> List[str]:
        return ["h_product_pk"]


@dataclass
class H_Category(MODEL):
    h_category_pk: UUID
    category_name: str
    load_dt: datetime
    load_src: str

    def get_table_name(self) -> str:
        return "dds.h_category"

    def get_unique_columns(self) -> List[str]:
        return ["h_category_pk"]


@dataclass
class H_Restaurant(MODEL):
    h_restaurant_pk: UUID
    restaurant_id: str
    load_dt: datetime
    load_src: str

    def get_table_name(self) -> str:
        return "dds.h_restaurant"

    def get_unique_columns(self) -> List[str]:
        return ["h_restaurant_pk"]


@dataclass
class H_Order(MODEL):
    h_order_pk: UUID
    order_id: str
    order_dt: datetime
    load_dt: datetime
    load_src: str

    def get_table_name(self) -> str:
        return "dds.h_order"

    def get_unique_columns(self) -> List[str]:
        return ["h_order_pk"]


@dataclass
class S_User_Names(MODEL):
    h_user_pk: UUID
    username: str
    userlogin: str
    load_dt: datetime
    load_src: str
    hk_user_names_hashdiff: UUID

    def get_table_name(self) -> str:
        return "dds.s_user_names"

    def get_unique_columns(self) -> List[str]:
        return ["hk_user_names_hashdiff"]


@dataclass
class S_Product_Names(MODEL):
    h_product_pk: UUID
    name: str
    load_dt: datetime
    load_src: str
    hk_product_names_hashdiff: UUID

    def get_table_name(self) -> str:
        return "dds.s_product_names"

    def get_unique_columns(self) -> List[str]:
        return ["hk_product_names_hashdiff"]


@dataclass
class S_Restaurant_Names(MODEL):
    h_restaurant_pk: UUID
    name: str
    load_dt: datetime
    load_src: str
    hk_restaurant_names_hashdiff: UUID

    def get_table_name(self) -> str:
        return "dds.s_restaurant_names"

    def get_unique_columns(self) -> List[str]:
        return ["hk_restaurant_names_hashdiff"]


@dataclass
class S_Order_Cost(MODEL):
    h_order_pk: UUID
    cost: float
    payment: float
    load_dt: datetime
    load_src: str
    hk_order_cost_hashdiff: UUID

    def get_table_name(self) -> str:
        return "dds.s_order_cost"

    def get_unique_columns(self) -> List[str]:
        return ["hk_order_cost_hashdiff"]


@dataclass
class S_Order_Status(MODEL):
    h_order_pk: UUID
    status: str
    load_dt: datetime
    load_src: str
    hk_order_status_hashdiff: UUID

    def get_table_name(self) -> str:
        return "dds.s_order_status"

    def get_unique_columns(self) -> List[str]:
        return ["hk_order_status_hashdiff"]


@dataclass
class L_Order_Product(MODEL):
    hk_order_product_pk: UUID
    h_order_pk: UUID
    h_product_pk: UUID
    load_dt: datetime
    load_src: str

    def get_table_name(self) -> str:
        return "dds.l_order_product"

    def get_unique_columns(self) -> List[str]:
        return ["hk_order_product_pk"]


@dataclass
class L_Product_Restaurant(MODEL):
    hk_product_restaurant_pk: UUID
    h_product_pk: UUID
    h_restaurant_pk: UUID
    load_dt: datetime
    load_src: str

    def get_table_name(self) -> str:
        return "dds.l_product_restaurant"

    def get_unique_columns(self) -> List[str]:
        return ["hk_product_restaurant_pk"]


@dataclass
class L_Product_Category(MODEL):
    hk_product_category_pk: UUID
    h_product_pk: UUID
    h_category_pk: UUID
    load_dt: datetime
    load_src: str

    def get_table_name(self) -> str:
        return "dds.l_product_category"

    def get_unique_columns(self) -> List[str]:
        return ["hk_product_category_pk"]


@dataclass
class L_Order_User(MODEL):
    hk_order_user_pk: UUID
    h_order_pk: UUID
    h_user_pk: UUID
    load_dt: datetime
    load_src: str

    def get_table_name(self) -> str:
        return "dds.l_order_user"

    def get_unique_columns(self) -> List[str]:
        return ["hk_order_user_pk"]
