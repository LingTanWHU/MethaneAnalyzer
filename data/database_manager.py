import sqlite3
import pandas as pd
from datetime import datetime
import os
from typing import List, Optional
import hashlib
import glob
import pytz

class DatabaseManager:
    def __init__(self, db_path: str = "gas_data.db"):
        self.db_path = db_path
        self.init_database()
    
    # 在 init_database 中，修改表结构
    def init_database(self):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # 修改预处理数据表：添加 source_file_name
        for data_type in ['picarro', 'pico']:
            table_name = f"{data_type}_processed_data"
            # 先创建表（如果不存在）
            if data_type == 'picarro':
                cols = """
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    DATETIME TEXT,
                    CO2_dry REAL,
                    CH4_dry REAL,
                    H2O REAL,
                    CO2 REAL,
                    CH4 REAL,
                    CO2_dry_std REAL,
                    CH4_dry_std REAL,
                    H2O_std REAL,
                    CO2_std REAL,
                    CH4_std REAL,
                    time_window TEXT,
                    agg_method TEXT,
                    source_file_name TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                """
            else:
                cols = """
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    DATETIME TEXT,
                    CH4 REAL,
                    C2H6 REAL,
                    H2O REAL,
                    Tgas REAL,
                    CH4_std REAL,
                    C2H6_std REAL,
                    H2O_std REAL,
                    Tgas_std REAL,
                    time_window TEXT,
                    agg_method TEXT,
                    source_file_name TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                """
            
            cursor.execute(f'CREATE TABLE IF NOT EXISTS {table_name} ({cols})')
            
            # 添加索引（如果 source_file_name 列不存在，则添加）
            try:
                cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN source_file_name TEXT")
            except sqlite3.OperationalError:
                # 列已存在
                pass
            
            # 创建索引
            cursor.execute(f'CREATE INDEX IF NOT EXISTS idx_{data_type}_proc_file ON {table_name}(source_file_name)')
            cursor.execute(f'CREATE INDEX IF NOT EXISTS idx_{data_type}_proc_datetime ON {table_name}(DATETIME)')
            cursor.execute(f'CREATE INDEX IF NOT EXISTS idx_{data_type}_proc_time_window ON {table_name}(time_window, agg_method)')
        
        # 修改 file_records 表：主键改为 file_name
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS file_records (
                file_name TEXT PRIMARY KEY,
                original_path TEXT,  -- 保留原始路径用于日志（可选）
                file_hash TEXT,
                last_modified TIMESTAMP,
                data_type TEXT,
                record_count INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        conn.commit()
        conn.close()
    
    def calculate_file_hash(self, file_path: str) -> str:
        """计算文件的MD5哈希值"""
        hash_md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    
    def get_existing_file_records(self) -> dict:
        """获取数据库中已有的文件记录，key 为文件名"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT file_name, file_hash, last_modified FROM file_records")
        records = {row[0]: {'hash': row[1], 'modified': row[2]} for row in cursor.fetchall()}
        conn.close()
        return records
    

    def delete_processed_data_by_file_name(self, file_name: str, data_type: str, time_window: str, agg_method: str):
        """删除指定文件名的预处理数据"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        table_name = f"{data_type}_processed_data"
        cursor.execute(f'''
            DELETE FROM {table_name}
            WHERE source_file_name = ? AND time_window = ? AND agg_method = ?
        ''', (file_name, time_window, agg_method))
        conn.commit()
        conn.close()

    def update_file_record(self, file_name: str, original_path: str, file_hash: str, data_type: str, record_count: int):
        """更新文件记录（按文件名）"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        modified_time = datetime.fromtimestamp(os.path.getmtime(original_path)).isoformat()
        
        cursor.execute('''
            INSERT OR REPLACE INTO file_records 
            (file_name, original_path, file_hash, last_modified, data_type, record_count, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ''', (file_name, original_path, file_hash, modified_time, data_type, record_count))
        
        conn.commit()
        conn.close()
    
    def insert_processed_data_to_db(self, df: pd.DataFrame, df_std: pd.DataFrame, 
                                data_type: str, time_window: str, agg_method: str,
                                source_file_name: str):
        """将预处理后的数据插入数据库，并标记来源文件名"""
        conn = sqlite3.connect(self.db_path)
        
        df_complete = df.copy()
        
        # 添加标准差列
        if not df_std.empty:
            for col in df_std.columns:
                if col != 'DATETIME' and col in df.columns:
                    std_col_name = f"{col}_std"
                    df_complete[std_col_name] = df_std[col]
        
        # 选择列
        if data_type == 'picarro':
            required_columns = ['DATETIME', 'CO2_dry', 'CH4_dry', 'H2O', 'CO2', 'CH4', 
                            'CO2_dry_std', 'CH4_dry_std', 'H2O_std', 'CO2_std', 'CH4_std']
        else:
            required_columns = ['DATETIME', 'CH4', 'C2H6', 'H2O', 'Tgas',
                            'CH4_std', 'C2H6_std', 'H2O_std', 'Tgas_std']
        
        available_columns = [col for col in required_columns if col in df_complete.columns]
        if not available_columns:
            conn.close()
            return
        
        df_filtered = df_complete[available_columns].copy()
        df_filtered['time_window'] = time_window
        df_filtered['agg_method'] = agg_method
        df_filtered['source_file_name'] = source_file_name  # ← 关键新增
        
        table_name = f"{data_type}_processed_data"
        df_filtered.to_sql(table_name, conn, if_exists='append', index=False)
        conn.close()
    
    def query_processed_data_from_db(self, data_type: str, start_time: str, end_time: str, 
                                   time_window: str, agg_method: str) -> pd.DataFrame:
        """从数据库查询预处理数据"""
        conn = sqlite3.connect(self.db_path)
        
        table_name = f"{data_type}_processed_data"
        query = f'''
            SELECT * FROM {table_name} 
            WHERE DATETIME BETWEEN ? AND ? 
            AND time_window = ? 
            AND agg_method = ?
            ORDER BY DATETIME
        '''
        
        df = pd.read_sql_query(query, conn, params=(start_time, end_time, time_window, agg_method))
        conn.close()
        
        if not df.empty and 'DATETIME' in df.columns:
            df['DATETIME'] = pd.to_datetime(df['DATETIME'])
        
        return df