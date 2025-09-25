# pg_pipeline.py

from __future__ import annotations

import datetime as dt

from typing import Any, Dict, Optional, Sequence, List



import psycopg

from psycopg.rows import dict_row

from psycopg import sql





class PostgresPipeline:

    """

    Scrapy item pipeline for PostgreSQL.



    New features:

      - PG_USE_EXISTING_TABLE: when True, never create table/index; inspect columns and insert only.

      - PG_FIELD_MAP: item_field -> db_column mapping.

      - PG_STATIC_FIELDS: extra constant columns to insert.

      - PG_STRICT_COLUMNS: only insert columns that exist in table (ignore unknowns if False).

    """



    def __init__(

        self,

        dsn: str,

        table: Optional[str] = None,

        upsert_keys: Optional[Sequence[str]] = None,

        schema: Optional[str] = None,

        create_index_on_upsert_keys: bool = True,

        batch_size: int = 1,

        use_existing_table: bool = False,

        field_map: Optional[Dict[str, str]] = None,

        static_fields: Optional[Dict[str, Any]] = None,

        strict_columns: bool = True,

    ) -> None:

        self.dsn = dsn

        self.table = table

        self.upsert_keys = list(upsert_keys) if upsert_keys else None

        self.schema = schema

        self.create_index_on_upsert_keys = create_index_on_upsert_keys

        self.batch_size = max(1, batch_size)



        self.use_existing_table = use_existing_table

        self.field_map = field_map or {}

        self.static_fields = static_fields or {}

        self.strict_columns = strict_columns



        self.conn: Optional[psycopg.Connection] = None

        self.cur: Optional[psycopg.Cursor] = None



        self._created: bool = False

        self._col_types: Dict[str, str] = {}

        self._buffer: List[Dict[str, Any]] = []

        self._table_columns: Optional[set[str]] = None

        self._table_states: Dict[str, Dict[str, Any]] = {}

        self.target_table: Optional[str] = None



    def _apply_spider_overrides(self, spider) -> None:

        """Allow spiders to override PG* settings via attributes or a dict."""

        attr_map = {

            'pg_dsn': 'dsn',

            'pg_table': 'table',

            'pg_schema': 'schema',

            'pg_upsert_keys': 'upsert_keys',

            'pg_create_index_on_upsert_keys': 'create_index_on_upsert_keys',

            'pg_batch_size': 'batch_size',

            'pg_use_existing_table': 'use_existing_table',

            'pg_field_map': 'field_map',

            'pg_static_fields': 'static_fields',

            'pg_strict_columns': 'strict_columns',

        }



        def apply(attr: str, value):

            if value is None:

                return

            if attr == 'upsert_keys' and value is not None:

                value = list(value) if value else []

            elif attr == 'batch_size':

                value = max(1, int(value))

            elif attr in {'field_map', 'static_fields'}:

                value = dict(value)

            setattr(self, attr, value)



        config = getattr(spider, 'pg_pipeline', None)

        if callable(config):

            config = config()

        if isinstance(config, dict):

            for key, attr in attr_map.items():

                if key in config:

                    apply(attr, config[key])



        for key, attr in attr_map.items():

            if hasattr(spider, key):

                apply(attr, getattr(spider, key))



    @classmethod

    def from_crawler(cls, crawler):

        s = crawler.settings



        dsn = s.get("PG_DSN")

        if not dsn:

            parts = {

                "host": s.get("PG_HOST", "localhost"),

                "port": s.getint("PG_PORT", 5432),

                "dbname": s.get("PG_DATABASE") or s.get("PG_DB"),

                "user": s.get("PG_USER"),

                "password": s.get("PG_PASSWORD"),

            }

            if not parts["dbname"]:

                raise ValueError("PG_DSN or PG_DATABASE/PG_DB must be configured.")

            dsn = " ".join(f"{k}={v}" for k, v in parts.items() if v is not None)



        return cls(

            dsn=dsn,

            table=s.get("PG_TABLE"),

            upsert_keys=s.getlist("PG_UPSERT_KEYS") or None,

            schema=s.get("PG_SCHEMA"),

            create_index_on_upsert_keys=s.getbool("PG_CREATE_INDEX_ON_UPSERT_KEYS", True),

            batch_size=s.getint("PG_BATCH_SIZE", 1),

            use_existing_table=s.getbool("PG_USE_EXISTING_TABLE", False),

            field_map=s.getdict("PG_FIELD_MAP", {}),

            static_fields=s.getdict("PG_STATIC_FIELDS", {}),

            strict_columns=s.getbool("PG_STRICT_COLUMNS", True),

        )



    # ---------- Scrapy lifecycle ----------

    def open_spider(self, spider):
        self._apply_spider_overrides(spider)
        if not self.dsn:
            raise ValueError("PG_DSN must be configured via settings or spider overrides.")
        self.conn = psycopg.connect(self.dsn, autocommit=False, row_factory=dict_row)
        self.cur = self.conn.cursor()
        self._table_states = {}
        self.target_table = self.table or spider.name

    def _ensure_table_state(self, table: str) -> Dict[str, Any]:
        state = self._table_states.get(table)
        if state is None:
            state = {"buffer": [], "col_types": {}, "table_columns": None, "created": False}
            self._table_states[table] = state
        if self.use_existing_table and state["table_columns"] is None:
            cols = self._fetch_table_columns(table)
            if not cols:
                raise RuntimeError(f"Table not found or has no columns: {self.schema or 'public'}.{table}")
            state["table_columns"] = cols
            state["created"] = True
        self.target_table = table
        self._buffer = state["buffer"]
        self._col_types = state["col_types"]
        self._table_columns = state["table_columns"]
        self._created = state["created"]
        return state

    def _sync_state(self, table: str) -> None:
        state = self._table_states[table]
        state["buffer"] = self._buffer
        state["col_types"] = self._col_types
        state["table_columns"] = self._table_columns
        state["created"] = self._created


    def close_spider(self, spider):
        try:
            for table in list(self._table_states.keys()):
                self._ensure_table_state(table)
                self._flush()
                self._sync_state(table)
        finally:
            if self.cur:
                self.cur.close()
            if self.conn:
                self.conn.commit()
                self.conn.close()

    def process_item(self, item, spider):
        data = dict(item) if not isinstance(item, dict) else item

        table_override = data.pop("_pg_table", None)
        skip_pg = data.pop("_pg_skip", False) or data.pop("_pg_skip_pg", False)
        if skip_pg:
            return item

        target_table = table_override or (self.table or spider.name)
        if not target_table:
            return item

        self._ensure_table_state(target_table)

        # Apply mapping
        mapped: Dict[str, Any] = {}
        for k, v in data.items():
            col = self.field_map.get(k, k)
            mapped[col] = v

        # Add static fields
        for k, v in self.static_fields.items():
            mapped[k] = v

        # If using existing table, prune to available columns
        if self.use_existing_table:
            assert self._table_columns is not None
            if self.strict_columns:
                mapped = {k: mapped[k] for k in list(mapped.keys()) if k in self._table_columns}
            else:
                mapped = {k: v for k, v in mapped.items() if k in self._table_columns}

        if not mapped:
            self._sync_state(target_table)
            return item

        if not self.use_existing_table and not self._created:
            self._infer_column_types(mapped)
            self._ensure_schema_and_table_exists()
        self._sync_state(target_table)

        self._buffer.append(mapped)
        if len(self._buffer) >= self.batch_size:
            self._flush()
        self._sync_state(target_table)
        return item

    # ---------- Internals ----------

    def _fetch_table_columns(self, table: str) -> set[str]:
        assert self.cur is not None
        if self.schema:
            q = """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
            """
            self.cur.execute(q, (self.schema, table))
        else:
            q = """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = current_schema() AND table_name = %s
            """
            self.cur.execute(q, (table,))
        rows = self.cur.fetchall()
        return {r["column_name"] for r in rows}


    def _infer_column_types(self, sample: Dict[str, Any]) -> None:

        def guess(v: Any) -> str:

            if isinstance(v, bool):

                return "BOOLEAN"

            if isinstance(v, int) and not isinstance(v, bool):

                return "BIGINT"

            if isinstance(v, float):

                return "DOUBLE PRECISION"

            if isinstance(v, dt.datetime):

                return "TIMESTAMP"

            if isinstance(v, dt.date):

                return "DATE"

            if isinstance(v, (list, dict)):

                return "JSONB"

            return "TEXT"



        for k, v in sample.items():

            self._col_types[k] = guess(v)



        # Ensure upsert keys exist as columns if needed

        if self.upsert_keys:

            for k in self.upsert_keys:

                if k not in self._col_types:

                    self._col_types[k] = "TEXT"



    def _ensure_schema_and_table_exists(self) -> None:

        # Legacy: only when not using existing table

        assert self.cur is not None

        schema = self.schema

        table = self.target_table



        if schema:

            self.cur.execute(

                sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema))

            )



        cols_def = sql.SQL(", ").join(

            sql.SQL("{} {}").format(sql.Identifier(name), sql.SQL(typ))

            for name, typ in self._col_types.items()

        )



        create_stmt = sql.SQL("CREATE TABLE IF NOT EXISTS {tbl} ({cols})").format(

            tbl=sql.SQL("{}.{}").format(sql.Identifier(schema), sql.Identifier(table))

            if schema else sql.Identifier(table),

            cols=cols_def,

        )

        self.cur.execute(create_stmt)



        if self.upsert_keys and self.create_index_on_upsert_keys:

            idx = f"ux_{(schema + '_' if schema else '')}{table}_" + "_".join(self.upsert_keys)

            self.cur.execute(

                sql.SQL("CREATE UNIQUE INDEX IF NOT EXISTS {} ON {tbl} ({cols})").format(

                    sql.Identifier(idx),

                    tbl=sql.SQL("{}.{}").format(sql.Identifier(schema), sql.Identifier(table))

                    if schema else sql.Identifier(table),

                    cols=sql.SQL(", ").join(sql.Identifier(k) for k in self.upsert_keys),

                )

            )



        self.conn.commit()

        self._created = True



    def _flush(self) -> None:

        if not self._buffer:

            return

        assert self.cur is not None



        rows = list(self._buffer)

        self._buffer.clear()



        # 统一列集合（防止每行字段不一致）

        all_cols = set()

        for r in rows:

            all_cols.update(r.keys())

        columns = sorted(all_cols)



        # 参数矩阵，自动把 list/dict 转成 JSON

        values_matrix = []

        for r in rows:

            row_vals = []

            for c in columns:

                v = r.get(c)

                if isinstance(v, (list, dict)):

                    row_vals.append(psycopg.types.json.Json(v))

                else:

                    row_vals.append(v)

            values_matrix.append(tuple(row_vals))



        tbl = (

            sql.SQL("{}.{}").format(sql.Identifier(self.schema), sql.Identifier(self.target_table))

            if self.schema else sql.Identifier(self.target_table)

        )



        ins = sql.SQL("INSERT INTO {tbl} ({cols}) VALUES {vals}").format(

            tbl=tbl,

            cols=sql.SQL(", ").join(sql.Identifier(c) for c in columns),

            vals=sql.SQL(", ").join(

                sql.SQL("({})").format(sql.SQL(", ").join(sql.Placeholder() for _ in columns))

                for _ in values_matrix

            ),

        )



        # 保留你原本的 UPSERT 行为（如果设置了 upsert_keys）

        if getattr(self, "upsert_keys", None):

            set_clause_parts = []

            for c in columns:

                if c in self.upsert_keys:

                    continue

                set_clause_parts.append(

                    sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(c), sql.Identifier(c))

                )

            if set_clause_parts:

                on_conflict = sql.SQL(" ON CONFLICT ({keys}) DO UPDATE SET {setc}").format(

                    keys=sql.SQL(", ").join(sql.Identifier(k) for k in self.upsert_keys),

                    setc=sql.SQL(", ").join(set_clause_parts),

                )

            else:

                on_conflict = sql.SQL(" ON CONFLICT ({keys}) DO NOTHING").format(

                    keys=sql.SQL(", ").join(sql.Identifier(k) for k in self.upsert_keys)

                )

            ins = sql.Composed([ins, on_conflict])



        # ====== 新增：批量失败时自动回滚并逐条定位坏数据 ======

        try:

            # 尝试批量插入

            flat_params = []

            for tup in values_matrix:

                flat_params.extend(tup)

            self.cur.execute(ins, flat_params)

            self.conn.commit()

        except Exception as e:

            # 事务已失败，先回滚以解除 "current transaction is aborted"

            self.conn.rollback()

            # 打印批量错误的关键信息

            print("[PG][BatchInsertError]", type(e).__name__, getattr(e, "pgcode", None), getattr(e, "pgerror", str(e)))



            # 逐条尝试，找到第一条真正的坏数据

            single_ins = sql.SQL("INSERT INTO {tbl} ({cols}) VALUES ({vals})").format(

                tbl=tbl,

                cols=sql.SQL(", ").join(sql.Identifier(c) for c in columns),

                vals=sql.SQL(", ").join(sql.Placeholder() for _ in columns),

            )

            if getattr(self, "upsert_keys", None):

                single_ins = sql.Composed([single_ins, on_conflict])



            for row in values_matrix:

                try:

                    self.cur.execute(single_ins, row)

                    self.conn.commit()

                except Exception as ee:

                    self.conn.rollback()

                    # 打印问题数据和真正错误

                    print("[PG][BadRow]", dict(zip(columns, row)))

                    print("[PG][BadRowError]", type(ee).__name__, getattr(ee, "pgcode", None), getattr(ee, "pgerror", str(ee)))

                    # 抛出让 Scrapy 显示，方便你看到

                    raise

