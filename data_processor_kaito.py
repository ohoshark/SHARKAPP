import sqlite3
import json
import os
import pandas as pd
from datetime import datetime

class DataProcessorKaito:
    """Kaito 프로젝트용 통합 DB 데이터 프로세서"""
    
    def __init__(self, db_path='./data/kaito/kaito_projects.db'):
        self.db_path = db_path
        self.base_dir = './data/kaito/'
        
        # DB 디렉토리 생성
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        
        # DB 초기화
        self.create_tables()
        
        # 처리된 파일 추적
        self.processed_files = {}  # {project: {timeframe: set(filenames)}}
        self.load_processed_files()
    
    def create_tables(self):
        """데이터베이스 테이블 생성"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # 순위 데이터 테이블
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS rankings (
                    projectName TEXT,
                    timeframe TEXT,
                    timestamp TEXT,
                    rank INTEGER,
                    handle TEXT,
                    displayName TEXT,
                    imageId TEXT,
                    mindshare TEXT,
                    smartFollower TEXT,
                    follower TEXT,
                    PRIMARY KEY (projectName, timeframe, timestamp, handle)
                )
            ''')
            
            # 인덱스 생성
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_project_timeframe 
                ON rankings(projectName, timeframe)
            ''')
            
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_handle 
                ON rankings(handle)
            ''')
            
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_timestamp 
                ON rankings(projectName, timeframe, timestamp)
            ''')
            
            conn.commit()
    
    def load_processed_files(self):
        """이미 처리된 파일 목록 로드"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT DISTINCT projectName, timeframe, timestamp FROM rankings')
            rows = cursor.fetchall()
            
            for project, timeframe, timestamp in rows:
                if project not in self.processed_files:
                    self.processed_files[project] = {}
                if timeframe not in self.processed_files[project]:
                    self.processed_files[project][timeframe] = set()
                self.processed_files[project][timeframe].add(timestamp)
    
    def scan_projects(self):
        """모든 Kaito 프로젝트 스캔"""
        projects = []
        
        if not os.path.exists(self.base_dir):
            return projects
        
        for project_name in os.listdir(self.base_dir):
            project_path = os.path.join(self.base_dir, project_name)
            
            # 디렉토리이고, 숨김 파일이 아니며, DB 파일이 아닌 경우
            if (os.path.isdir(project_path) and 
                not project_name.startswith('_') and 
                not project_name.startswith('.') and
                project_name != 'kaito_projects.db'):
                
                global_path = os.path.join(project_path, 'global')
                if os.path.isdir(global_path):
                    projects.append(project_name)
        
        return sorted(projects)
    
    def check_new_files(self, project_name, timeframe):
        """신규 JSON 파일 체크"""
        new_files = []
        
        timeframe_dir = os.path.join(self.base_dir, project_name, 'global', timeframe)
        
        if not os.path.exists(timeframe_dir):
            return new_files
        
        # 처리된 파일 목록 가져오기
        processed = self.processed_files.get(project_name, {}).get(timeframe, set())
        
        # JSON 파일 스캔
        for filename in os.listdir(timeframe_dir):
            if filename.endswith('.json'):
                filepath = os.path.join(timeframe_dir, filename)
                
                # 타임스탬프 추출 (파일명에서)
                timestamp = filename.replace('.json', '').replace('_', '-').replace(' ', ' ')
                
                # 이미 처리된 파일인지 확인
                if timestamp not in processed:
                    new_files.append(filepath)
        
        return new_files
    
    def load_json_file(self, filepath):
        """JSON 파일 로드"""
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"[Kaito] 파일 로드 실패: {filepath} - {e}")
            return None
    
    def insert_data(self, project_name, timeframe, timestamp, data):
        """데이터 삽입"""
        if not data:
            return
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            for item in data:
                cursor.execute('''
                    INSERT OR REPLACE INTO rankings 
                    (projectName, timeframe, timestamp, rank, handle, displayName, imageId, mindshare, smartFollower, follower)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    project_name,
                    timeframe,
                    timestamp,
                    int(item.get('rank', 0)),
                    item.get('handle', ''),
                    item.get('displayName', ''),
                    item.get('imageId', ''),
                    item.get('mindshare', ''),
                    item.get('smartFollower', ''),
                    item.get('follower', '')
                ))
            
            conn.commit()
        
        # 처리된 파일 추적
        if project_name not in self.processed_files:
            self.processed_files[project_name] = {}
        if timeframe not in self.processed_files[project_name]:
            self.processed_files[project_name][timeframe] = set()
        self.processed_files[project_name][timeframe].add(timestamp)
    
    def get_available_timestamps(self, project_name, timeframe):
        """사용 가능한 타임스탬프 목록"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT DISTINCT timestamp 
                FROM rankings 
                WHERE projectName = ? AND timeframe = ?
                ORDER BY timestamp
            ''', (project_name, timeframe))
            
            return [row[0] for row in cursor.fetchall()]
    
    def get_available_timeframes(self, project_name):
        """사용 가능한 timeframe 목록"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT DISTINCT timeframe 
                FROM rankings 
                WHERE projectName = ?
            ''', (project_name,))
            
            timeframes = [row[0] for row in cursor.fetchall()]
            
            # 정렬: 7D, 30D, 90D, 180D, 360D 순서
            order = {'7D': 0, '30D': 1, '90D': 2, '180D': 3, '360D': 4}
            return sorted(timeframes, key=lambda x: order.get(x, 999))
    
    def compare_leaderboards(self, project_name, timestamp1, timestamp2, timeframe):
        """두 타임스탬프의 리더보드 비교"""
        with sqlite3.connect(self.db_path) as conn:
            query = '''
                SELECT 
                    COALESCE(t1.handle, t2.handle) as handle,
                    COALESCE(t1.displayName, t2.displayName) as displayName,
                    COALESCE(t1.imageId, t2.imageId) as imageId,
                    COALESCE(t1.rank, 9999) as prev_rank,
                    COALESCE(t2.rank, 9999) as curr_rank,
                    COALESCE(t1.rank, 9999) - COALESCE(t2.rank, 9999) as rank_change,
                    COALESCE(t1.mindshare, '0%') as prev_mindshare,
                    COALESCE(t2.mindshare, '0%') as curr_mindshare,
                    COALESCE(t1.smartFollower, '0') as prev_smartFollower,
                    COALESCE(t2.smartFollower, '0') as curr_smartFollower,
                    COALESCE(t1.follower, '0') as prev_follower,
                    COALESCE(t2.follower, '0') as curr_follower
                FROM
                    (SELECT * FROM rankings WHERE projectName = ? AND timeframe = ? AND timestamp = ?) t1
                FULL OUTER JOIN
                    (SELECT * FROM rankings WHERE projectName = ? AND timeframe = ? AND timestamp = ?) t2
                ON t1.handle = t2.handle
                ORDER BY curr_rank
            '''
            
            df = pd.read_sql_query(query, conn, params=(
                project_name, timeframe, timestamp1,
                project_name, timeframe, timestamp2
            ))
            
            return df
    
    def get_user_data(self, project_name, handle, timeframe):
        """특정 사용자의 시간별 데이터"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT timestamp, rank, displayName, imageId, mindshare, smartFollower, follower
                FROM rankings
                WHERE projectName = ? AND timeframe = ? AND handle = ?
                ORDER BY timestamp
            ''', (project_name, timeframe, handle))
            
            rows = cursor.fetchall()
            
            if not rows:
                return pd.DataFrame()
            
            df = pd.DataFrame(rows, columns=['timestamp', 'rank', 'displayName', 'imageId', 'mindshare', 'smartFollower', 'follower'])
            # Convert timestamp format: 2026-0102-190000 or 2026_0102_190000 -> 2026-01-02 19:00:00
            # Remove all non-numeric characters (hyphens and underscores)
            df['timestamp'] = df['timestamp'].str.replace(r'[-_]', '', regex=True)
            df['timestamp'] = pd.to_datetime(df['timestamp'], format='%Y%m%d%H%M%S')
            
            return df
    
    def get_user_info(self, project_name, handle):
        """사용자 최신 정보"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT displayName, imageId, rank, mindshare, smartFollower, follower
                FROM rankings
                WHERE projectName = ? AND handle = ?
                ORDER BY timestamp DESC
                LIMIT 1
            ''', (project_name, handle))
            
            row = cursor.fetchone()
            
            if not row:
                return {}
            
            return {
                'displayName': row[0],
                'imageId': row[1],
                'rank': row[2],
                'mindshare': row[3],
                'smartFollower': row[4],
                'follower': row[5],
                'handle': handle
            }
    
    def get_all_handles(self, project_name, timeframe=None):
        """모든 사용자 핸들 목록"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            if timeframe:
                cursor.execute('''
                    SELECT DISTINCT handle
                    FROM rankings
                    WHERE projectName = ? AND timeframe = ?
                    ORDER BY handle
                ''', (project_name, timeframe))
            else:
                cursor.execute('''
                    SELECT DISTINCT handle
                    FROM rankings
                    WHERE projectName = ?
                    ORDER BY handle
                ''', (project_name,))
            
            return [row[0] for row in cursor.fetchall()]
    
    def get_all_users(self, project_name, timeframe=None):
        """모든 사용자 정보 (handle, displayName)"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            if timeframe:
                cursor.execute('''
                    SELECT DISTINCT handle, displayName
                    FROM rankings
                    WHERE projectName = ? AND timeframe = ?
                    ORDER BY handle
                ''', (project_name, timeframe))
            else:
                cursor.execute('''
                    SELECT DISTINCT handle, displayName
                    FROM rankings
                    WHERE projectName = ?
                    ORDER BY handle
                ''', (project_name,))
            
            return [{'handle': row[0], 'displayName': row[1]} for row in cursor.fetchall()]
