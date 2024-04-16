class QueryBuilder:
    @classmethod
    def _replace_table(cls,
                       table: str, 
                       columns: list='*') -> str:
        return f"SELECT {columns} FROM {table}"
    
    @classmethod
    def _append_table(cls,
                      table: str, 
                      execution_date: str, 
                      columns: list='*') -> str:
        return f"SELECT {','.join(columns)} FROM {table} WHERE SUBSTR(DATE_ADD(createdAt, interval 9 hour), 1, 10) = '{execution_date}'"
    
    @classmethod
    def insert_into_table(cls,
                          table: str,
                          columns: list,
                          values: tuple) -> str:
        return f"INSERT INTO {table} ({','.join(columns)}) VALUES {values}"
    
    @classmethod
    def delete_from_table(cls,
                          table: str,
                          partition_id: str) -> str:
        return f"DELETE FROM {table} WHERE date_id='{partition_id}'"
    
    @classmethod
    def get_query(cls,
                  table: str, 
                  execution_date: str, 
                  columns: list='*') -> str:
        if table in ['receipts', 'appointments', 'treatments', 'chartReceipts']:
            return cls._append_table(table, execution_date, columns)
        else:
            return cls._replace_table(table, columns)