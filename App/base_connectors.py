from contextlib import contextmanager
from typing import Dict, Any, Optional
import pandas as pd
from sqlalchemy import text
from typing import Dict, Any
from utilities import logger

class BaseConnector:
    def __init__(self, config: Dict[str, Any]):
        self.config = config

    @contextmanager
    def _connect(self):
        raise NotImplementedError("_connect method not implemented")

    def _execute_transaction(self, conn, operation, transaction_mode='read_write'):
        if transaction_mode in ('write', 'read_write'):
            try:
                conn.execute(text("START TRANSACTION"))
                operation(conn)
                conn.execute(text("COMMIT"))
                logger.info("Transaction committed successfully.")
            except Exception as e:
                conn.execute(text("ROLLBACK"))
                logger.error(f"Transaction rolled back. Error: {e}")
                raise

    def read(self, query: Optional[str] = None) -> pd.DataFrame:
        raise NotImplementedError("read method not implemented")

    def write(self, df: pd.DataFrame, table_name: str, mode: str = 'append') -> None:
        raise NotImplementedError("write method not implemented")

    def table_exists(self, table_name: str) -> bool:
        raise NotImplementedError("table_exists method not implemented")

    def execute_query(self, query: str) -> None:
        with self._connect() as conn:
            self._execute_transaction(conn, lambda c: c.execute(text(query)))
