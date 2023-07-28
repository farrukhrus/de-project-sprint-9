from typing import List, Tuple

from dds_loader.repository.dds_model import MODEL
from lib.pg import PgConnect


class LOAD:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def insert(self, model: MODEL) -> None:
        table_name = model.get_table_name()
        columns = model.get_unique_columns()
        column_names = list(model.__fields__.keys())
        column_names_str = ", ".join(column_names)
        unique_columns = ", ".join(columns)
        values = ", ".join([f"%({name})s" for name in column_names])
        updates = ", ".join(
            [f"{name}=excluded.{name}" for name in column_names]
        )

        query = f"""
            insert into {table_name} ({column_names_str})
            values ({values})
            on conflict ({unique_columns})
            do update set {updates};
        """

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, model.dict())

    def counter(self, user_id: str, type: str) -> List[Tuple]:
        if type == "product":
            query = f"""
                select
                    lou.h_user_pk as user_id,
                    lop.h_product_pk as product_id,
                    spn.name as product_name,
                    count(*) as order_cnt
                from dds.l_order_product lop
                join dds.l_order_user lou on lou.h_order_pk=lop.h_order_pk
                join dds.s_product_names spn on lop.h_product_pk=spn.h_product_pk
                where lou.h_user_pk='{user_id}'
                group by user_id, product_id, product_name;
            """

        elif type == "category":
            query = f"""
                with temp as (
                    select
                    lpc.h_category_pk,
                    category_name,
                    h_product_pk
                    from dds.l_product_category lpc
                    join dds.h_category hc
                    on lpc.h_category_pk=hc.h_category_pk
                )
                select
                    lou.h_user_pk as user_id,
                    temp.h_category_pk as category_id,
                    temp.category_name as category_name,
                    count(lop.h_order_pk) as order_cnt
                    from dds.l_order_product lop
                    join dds.l_order_user lou
                    on lou.h_order_pk=lop.h_order_pk
                join temp
                on lop.h_product_pk=temp.h_product_pk
                where lou.h_user_pk='{user_id}'
                group by user_id, category_id, category_name;
            """

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
