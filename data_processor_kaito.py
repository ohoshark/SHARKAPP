import sqlite3
import json
import os
import glob
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
        
        # 처리된 파일 추적 (최신 파일만 추적)
        self.latest_file = {}  # {project: {timeframe: filename}}
        self.load_latest_files()
    
    def create_tables(self):
        """데이터베이스 테이블 생성"""
        with sqlite3.connect(self.db_path, timeout=30.0) as conn:
            cursor = conn.cursor()
            
            # WAL 모드 활성화 (쓰기 중에도 읽기 가능)
            cursor.execute('PRAGMA journal_mode=WAL')
            cursor.execute('PRAGMA busy_timeout=30000')  # 30초 타임아웃
            
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
            
            # 최신 파일 정보 테이블 추가
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS latest_files (
                    projectName TEXT,
                    timeframe TEXT,
                    filename TEXT,
                    PRIMARY KEY (projectName, timeframe)
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
            print("[Kaito DB] WAL 모드 활성화 완료 - 동시 읽기/쓰기 지원")
    
    def load_latest_files(self):
        """최신 파일 정보 로드"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT projectName, timeframe, filename FROM latest_files')
            rows = cursor.fetchall()
            
            for project, timeframe, filename in rows:
                if project not in self.latest_file:
                    self.latest_file[project] = {}
                self.latest_file[project][timeframe] = filename
    
    def save_latest_file(self, project_name, timeframe, filename):
        """최신 파일 정보 저장"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO latest_files (projectName, timeframe, filename)
                VALUES (?, ?, ?)
            ''', (project_name, timeframe, filename))
            conn.commit()
        
        # 메모리 업데이트
        if project_name not in self.latest_file:
            self.latest_file[project_name] = {}
        self.latest_file[project_name][timeframe] = filename
    
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
        
        # 최신 파일명 가져오기
        latest_filename = self.latest_file.get(project_name, {}).get(timeframe, "")
        
        # 타임스탬프 정규화 함수 (cleanup과 동일)
        def normalize_timestamp(filename):
            """파일명에서 타임스탬프 부분만 추출하여 숫자로 변환"""
            try:
                name = filename.replace('.json', '')
                name = name.replace('_', '').replace('-', '')
                return name
            except:
                return filename
        
        latest_normalized = normalize_timestamp(latest_filename)
        
        # JSON 파일 스캔
        all_files = sorted(glob.glob(os.path.join(timeframe_dir, "*.json")))
        
        for filepath in all_files:
            filename = os.path.basename(filepath)
            filename_normalized = normalize_timestamp(filename)
            
            # 정규화된 타임스탬프로 비교 (최신 파일보다 새로운 파일만)
            if filename_normalized > latest_normalized:
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
    
    def insert_data_batch(self, batch_items):
        """배치 데이터 삽입 (여러 프로젝트/timeframe의 데이터를 한 번에 처리)
        
        Args:
            batch_items: [(project_name, timeframe, timestamp, data), ...]
        """
        if not batch_items:
            return
        
        # 모든 레코드를 한 번에 준비
        all_records = []
        files_to_save = {}  # {(project, timeframe): [filenames]}
        
        for project_name, timeframe, timestamp, data in batch_items:
            if not data:
                continue
            
            filename = f"{timestamp}.json"
            
            # 같은 project/timeframe의 모든 파일을 리스트로 저장
            key = (project_name, timeframe)
            if key not in files_to_save:
                files_to_save[key] = []
            files_to_save[key].append(filename)
            
            for item in data:
                all_records.append((
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
        
        # 한 번의 트랜잭션으로 모든 데이터 삽입
        if all_records:
            with sqlite3.connect(self.db_path, timeout=30.0) as conn:
                cursor = conn.cursor()
                cursor.executemany('''
                    INSERT OR REPLACE INTO rankings 
                    (projectName, timeframe, timestamp, rank, handle, displayName, imageId, mindshare, smartFollower, follower)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', all_records)
                conn.commit()
        
        # 최신 파일 정보 저장 및 정리 (각 project/timeframe의 가장 최신 파일만)
        for (project_name, timeframe), filenames in files_to_save.items():
            # 가장 최신 파일을 찾음 (파일명이 타임스탬프라서 문자열 비교로 가능)
            latest_filename = max(filenames)
            self.save_latest_file(project_name, timeframe, latest_filename)
            self.cleanup_old_files(project_name, timeframe)
    
    def insert_data(self, project_name, timeframe, timestamp, data):
        """데이터 삽입 및 파일 정리 (단일 항목용 - 호환성 유지)"""
        if not data:
            return
        
        with sqlite3.connect(self.db_path, timeout=30.0) as conn:
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
        
        # 파일명 생성 (타임스탬프 기반)
        filename = f"{timestamp}.json"
        
        # 최신 파일 정보 저장
        self.save_latest_file(project_name, timeframe, filename)
        
        # 구버전 파일 정리
        self.cleanup_old_files(project_name, timeframe)
    
    def cleanup_old_files(self, project_name, timeframe):
        """최신 파일보다 오래된 파일들 삭제"""
        timeframe_dir = os.path.join(self.base_dir, project_name, 'global', timeframe)
        
        if not os.path.exists(timeframe_dir):
            return
        
        # 최신 파일명 가져오기
        latest_filename = self.latest_file.get(project_name, {}).get(timeframe, "")
        
        if not latest_filename:
            return
        
        # 모든 JSON 파일 스캔
        all_files = glob.glob(os.path.join(timeframe_dir, "*.json"))
        
        if len(all_files) <= 1:
            return
        
        # 타임스탬프 정규화 함수 (2026_0103_100000 또는 2026-0103-100000 -> 20260103100000)
        def normalize_timestamp(filename):
            """파일명에서 타임스탬프 부분만 추출하여 숫자로 변환"""
            try:
                # .json 제거
                name = filename.replace('.json', '')
                # 언더스코어와 대시 모두 제거
                name = name.replace('_', '').replace('-', '')
                return name
            except:
                return filename
        
        latest_normalized = normalize_timestamp(latest_filename)
        
        deleted_count = 0
        for filepath in all_files:
            filename = os.path.basename(filepath)
            filename_normalized = normalize_timestamp(filename)
            
            # 정규화된 타임스탬프로 비교
            if filename_normalized < latest_normalized:
                try:
                    os.remove(filepath)
                    deleted_count += 1
                except Exception as e:
                    print(f"[Kaito] 파일 삭제 실패: {filepath} - {e}")
        
        if deleted_count > 0:
            print(f"[Kaito] {project_name}/{timeframe}: {deleted_count}개 구버전 파일 삭제")
    
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
        """
        모든 사용자 정보 (handle, displayName)
        - handle 기준으로 중복 제거
        - 7D timeframe의 displayName을 우선적으로 사용
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            if timeframe:
                # 특정 timeframe만 조회
                cursor.execute('''
                    SELECT handle, displayName, MAX(timestamp) as latest_ts
                    FROM rankings
                    WHERE projectName = ? AND timeframe = ?
                    GROUP BY handle
                    ORDER BY handle
                ''', (project_name, timeframe))
                return [{'handle': row[0], 'displayName': row[1]} for row in cursor.fetchall()]
            else:
                # 모든 timeframe에서 조회 (7D 우선)
                all_users = {}
                
                # 7D를 먼저 조회
                cursor.execute('''
                    SELECT handle, displayName, MAX(timestamp) as latest_ts
                    FROM rankings
                    WHERE projectName = ? AND timeframe = '7D'
                    GROUP BY handle
                ''', (project_name,))
                
                for row in cursor.fetchall():
                    all_users[row[0]] = {'handle': row[0], 'displayName': row[1]}
                
                # 나머지 timeframe에서 7D에 없는 사용자만 추가
                cursor.execute('''
                    SELECT handle, displayName, MAX(timestamp) as latest_ts
                    FROM rankings
                    WHERE projectName = ? AND timeframe != '7D'
                    GROUP BY handle
                ''', (project_name,))
                
                for row in cursor.fetchall():
                    if row[0] not in all_users:
                        all_users[row[0]] = {'handle': row[0], 'displayName': row[1]}
                
                return sorted(all_users.values(), key=lambda x: x['handle'])
