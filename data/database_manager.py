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
    
    def init_database(self):
        """初始化数据库表结构"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # 预处理数据表 - 存储平均值
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS picarro_processed_data (
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
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS pico_processed_data (
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
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # 文件记录表，跟踪每个文件的状态
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS file_records (
                file_path TEXT PRIMARY KEY,
                file_hash TEXT,
                last_modified TIMESTAMP,
                data_type TEXT,
                record_count INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # 为时间字段创建索引
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_picarro_proc_datetime ON picarro_processed_data(DATETIME)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_pico_proc_datetime ON pico_processed_data(DATETIME)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_picarro_proc_time_window ON picarro_processed_data(time_window, agg_method)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_pico_proc_time_window ON pico_processed_data(time_window, agg_method)')
        
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
        """获取数据库中已有的文件记录"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT file_path, file_hash, last_modified FROM file_records")
        records = {row[0]: {'hash': row[1], 'modified': row[2]} for row in cursor.fetchall()}
        conn.close()
        return records
    
    def delete_old_processed_data_by_time_window(self, data_type: str, time_window: str, agg_method: str):
        """删除指定时间窗口和聚合方法的预处理数据"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        table_name = f"{data_type}_processed_data"
        cursor.execute(f"DELETE FROM {table_name} WHERE time_window = ? AND agg_method = ?", (time_window, agg_method))
        conn.commit()
        conn.close()
    
    def update_file_record(self, file_path: str, file_hash: str, data_type: str, record_count: int):
        """更新文件记录"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        modified_time = datetime.fromtimestamp(os.path.getmtime(file_path)).isoformat()
        
        cursor.execute('''
            INSERT OR REPLACE INTO file_records 
            (file_path, file_hash, last_modified, data_type, record_count, updated_at)
            VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ''', (file_path, file_hash, modified_time, data_type, record_count))
        
        conn.commit()
        conn.close()
    
    def insert_processed_data_to_db(self, df: pd.DataFrame, df_std: pd.DataFrame, data_type: str, time_window: str, agg_method: str):
        """将预处理后的数据（平均值和标准差）插入数据库"""
        conn = sqlite3.connect(self.db_path)
        
        # 创建包含平均值和标准差的完整DataFrame
        df_complete = df.copy()
        
        # 为标准差数据添加_std后缀
        if not df_std.empty:
            for col in df_std.columns:
                if col != 'DATETIME' and col in df.columns:
                    std_col_name = f"{col}_std"
                    df_complete[std_col_name] = df_std[col]
        
        # 只保留数据库表中定义的列（排除id和时间戳列）
        if data_type == 'picarro':
            required_columns = ['DATETIME', 'CO2_dry', 'CH4_dry', 'H2O', 'CO2', 'CH4', 
                              'CO2_dry_std', 'CH4_dry_std', 'H2O_std', 'CO2_std', 'CH4_std']
        elif data_type == 'pico':
            required_columns = ['DATETIME', 'CH4', 'C2H6', 'H2O', 'Tgas',
                              'CH4_std', 'C2H6_std', 'H2O_std', 'Tgas_std']
        
        # 筛选出存在的列
        available_columns = [col for col in required_columns if col in df_complete.columns]
        
        if not available_columns:
            conn.close()
            return
        
        # 创建只包含需要列的DataFrame
        df_filtered = df_complete[available_columns].copy()
        
        # 添加时间窗口和聚合方法
        df_filtered['time_window'] = time_window
        df_filtered['agg_method'] = agg_method
        
        table_name = f"{data_type}_processed_data"
        
        # 插入数据
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