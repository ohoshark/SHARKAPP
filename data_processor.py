import os
import orjson
import sqlite3
import json
import pandas as pd
import numpy as np
from datetime import datetime
import glob
from collections import defaultdict

class DataProcessor:
    def __init__(self, data_dir):
        self.data_dir = data_dir
        self.timeframes = ['7D', '14D', '30D', 'TOTAL']
        # DB 파일 경로 설정
        self.db_path = os.path.join(data_dir, "project_data.db")
        
        # 1. DB 초기화 (테이블 및 인덱스 생성)
        self._init_db()
        
        # 2. 최신 파일 정보 로드 (AttributeError 해결 지점)
        self.latest_file = self._load_latest_file_info()

    def _init_db(self):
        """DB 연결 및 필요한 테이블/인덱스 생성"""
        with sqlite3.connect(self.db_path, check_same_thread=False) as conn:
            cursor = conn.cursor()
            
            # WAL 모드 활성화 (쓰기 중에도 읽기 가능)
            cursor.execute('PRAGMA journal_mode=WAL')
            cursor.execute('PRAGMA busy_timeout=30000')  # 30초 타임아웃
            
            # 메인 데이터 테이블 수정
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS snaps (
                    id TEXT,              -- 🚨 'id' 컬럼 추가
                    timeframe TEXT,
                    username TEXT,
                    displayName TEXT,
                    rank INTEGER,
                    cSnapsPercentRank INTEGER,
                    snapsPercent REAL,
                    cSnapsPercent REAL,
                    followers INTEGER,
                    smartFollowers INTEGER,
                    timestamp TEXT,
                    profileImageUrl TEXT
                )
            """)
            # 파일 동기화를 위한 메타데이터 테이블
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS metadata (
                    key TEXT PRIMARY KEY,
                    value TEXT
                )
            """)
            # 검색 및 조회를 위한 인덱스 최적화
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_user_tf ON snaps (username, timeframe)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_ts_tf ON snaps (timestamp, timeframe)")
            conn.commit()

    def _load_latest_file_info(self):
        """DB 메타데이터 테이블에서 마지막 로드된 파일명을 가져옵니다."""
        latest_info = {}
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                for tf in self.timeframes:
                    cursor.execute("SELECT value FROM metadata WHERE key = ?", (f"latest_file_{tf}",))
                    row = cursor.fetchone()
                    latest_info[tf] = row[0] if row else ""
        except:
            pass
        return latest_info

    def _save_latest_file_info(self, timeframe, filename):
        """마지막 로드된 파일명을 DB에 저장합니다."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("INSERT OR REPLACE INTO metadata (key, value) VALUES (?, ?)", 
                          (f"latest_file_{timeframe}", filename))
            conn.commit()

    def load_data(self, files_to_load=None):
        """신규 JSON 파일을 DB에 인서트하고 구버전 파일을 삭제합니다."""
        if files_to_load is None:
            files_to_load = self.check_for_new_data()

        if not files_to_load:
            return False

        new_data_found = False
        with sqlite3.connect(self.db_path) as conn:
            for timeframe, files in files_to_load.items():
                if not files: continue
                
                all_records = []
                for file_path in files:
                    try:
                        filename = os.path.basename(file_path)
                        parts = filename.split('_')
                        ts_str = f"{parts[0]}_{parts[1]}"
                        timestamp = datetime.strptime(ts_str, '%Y%m%d_%H%M%S').strftime('%Y-%m-%d %H:%M:%S')
                        
                        with open(file_path, 'rb') as f:
                            raw_data = orjson.loads(f.read())
                        
                        if 'result' in raw_data and 'data' in raw_data['result']:
                            snaps = raw_data['result']['data']['json'].get('snaps', [])
                            for snap in snaps:
                                snap['timeframe'] = timeframe
                                snap['timestamp'] = timestamp
                                all_records.append(snap)
                            
                            # 최신 파일 정보 갱신
                            self.latest_file[timeframe] = filename
                            self._save_latest_file_info(timeframe, filename)
                            new_data_found = True
                    except Exception as e:
                        print(f"Error parsing {file_path}: {e}")

                if all_records:
                    df = pd.DataFrame(all_records)
                    
                    # smartFollowersDetails 컬럼 제거 (용량 절약을 위해 개수만 저장)
                    if 'smartFollowersDetails' in df.columns:
                        df = df.drop('smartFollowersDetails', axis=1)
                    
                    # 복합 객체(list, dict)를 JSON 문자열로 변환 (추가된 로직)
                    for col in df.columns:
                        if df[col].apply(lambda x: isinstance(x, (list, dict))).any():
                            df[col] = df[col].apply(lambda x: orjson.dumps(x).decode('utf-8') if x is not None else None)

                    # DB 스키마 자동 업데이트
                    cursor = conn.cursor()
                    cursor.execute("PRAGMA table_info(snaps)")
                    existing_columns = [info[1] for info in cursor.fetchall()]
                    for col in df.columns:
                        if col not in existing_columns:
                            cursor.execute(f"ALTER TABLE snaps ADD COLUMN {col} TEXT")
                    
                    df.to_sql('snaps', conn, if_exists='append', index=False)
                    print(f"[{timeframe}] DB Insert Complete.")

        # 🚨 데이터 삽입이 완전히 끝난 후 파일 정리 실행
        if new_data_found:
            self.cleanup_old_files()
            
        return new_data_found

    # data_processor.py의 cleanup_old_files 메서드 수정

    def cleanup_old_files(self):
        """DB에 기록된 최신 파일보다 '과거'의 파일들만 삭제합니다."""
        print(f"--- [{self.data_dir}] 안전한 파일 정리 시작 ---")
        for tf in self.timeframes:
            path = os.path.join(self.data_dir, tf)
            if not os.path.exists(path): continue
            
            # DB가 기억하는 이 타임프레임의 최신 파일명 (기준점)
            latest_filename = self.latest_file.get(tf, "")
            if not latest_filename: continue
            
            # 폴더 내 모든 json 파일 리스트업
            all_files = glob.glob(os.path.join(path, "*.json"))
            
            for f_path in all_files:
                f_name = os.path.basename(f_path)
                
                # 🚨 [수정] != 대신 < 를 사용합니다.
                # '기준이 되는 최신 파일'보다 이름(시간)이 작은 파일만 삭제합니다.
                if f_name < latest_filename:
                    try:
                        os.remove(f_path)
                        # print(f"Deleted old file: {f_name}")
                    except Exception as e:
                        print(f"Failed to delete {f_name}: {e}")
                
                # 만약 f_name > latest_filename 이라면? 
                # -> 방금 막 들어온 따끈따끈한 새 파일이므로 삭제하지 않고 남겨둡니다.
                # -> 다음 주기(30초 후)에 load_data가 이를 발견하여 처리할 것입니다.

    def check_for_new_data(self):
        """새로 생성된 JSON 파일이 있는지 체크합니다."""
        new_files = defaultdict(list)
        any_new = False
        for tf in self.timeframes:
            path = os.path.join(self.data_dir, tf)
            if not os.path.exists(path): continue
            
            all_files = sorted(glob.glob(os.path.join(path, "*.json")))
            last_loaded = self.latest_file.get(tf, "")
            for f in all_files:
                if os.path.basename(f) > last_loaded:
                    new_files[tf].append(f)
                    any_new = True
        return new_files if any_new else {}

    # --- 데이터 조회 함수들 (main.py와 호환) ---

    def get_available_timestamps(self, timeframe='TOTAL'):
        query = "SELECT DISTINCT timestamp FROM snaps WHERE timeframe = ? ORDER BY timestamp ASC"
        with sqlite3.connect(self.db_path) as conn:
            df = pd.read_sql(query, conn, params=(timeframe,))
        return df['timestamp'].tolist()

    def get_leaderboard_at_timestamp(self, timestamp, timeframe='TOTAL'):
        query = """
            SELECT username, displayName, rank, cSnapsPercentRank, 
                   snapsPercent, cSnapsPercent, followers, 
                   profileImageUrl, timestamp, timeframe 
            FROM snaps WHERE timestamp = ? AND timeframe = ?
        """
        with sqlite3.connect(self.db_path) as conn:
            return pd.read_sql(query, conn, params=(timestamp, timeframe))

    def get_user_history(self, username, timeframe='TOTAL'):
        query = """
            SELECT displayName, timestamp , rank, cSnapsPercentRank, 
                   snapsPercent, cSnapsPercent
            FROM snaps WHERE username = ? AND timeframe = ? ORDER BY timestamp ASC
        """
        with sqlite3.connect(self.db_path) as conn:
            history = pd.read_sql(query, conn, params=(username, timeframe))
        if history.empty: return pd.DataFrame()
        history['timestamp'] = pd.to_datetime(history['timestamp'])
        if len(history) > 500:
            indices = np.linspace(0, len(history) - 1, 500).astype(int)
            history = history.iloc[indices]
        return history

    def get_all_usernames(self, timeframe='TOTAL'):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT MAX(timestamp) FROM snaps WHERE timeframe = ?", (timeframe,))
            latest_ts = cursor.fetchone()[0]
            if not latest_ts: return []
            query = "SELECT username, displayName FROM snaps WHERE timestamp = ? AND timeframe = ?"
            return pd.read_sql(query, conn, params=(latest_ts, timeframe)).to_dict('records')
    
    def get_all_usernames_from_multiple_timeframes(self, timeframes=['7D', '14D', '30D', 'TOTAL']):
        """여러 timeframe에서 사용자를 가져와 중복 제거 후 반환"""
        all_users = {}
        with sqlite3.connect(self.db_path) as conn:
            for tf in timeframes:
                cursor = conn.cursor()
                cursor.execute("SELECT MAX(timestamp) FROM snaps WHERE timeframe = ?", (tf,))
                latest_ts = cursor.fetchone()[0]
                if not latest_ts:
                    continue
                query = "SELECT username, displayName FROM snaps WHERE timestamp = ? AND timeframe = ?"
                users = pd.read_sql(query, conn, params=(latest_ts, tf)).to_dict('records')
                for user in users:
                    # username을 키로 사용하여 중복 제거
                    if user['username'] not in all_users:
                        all_users[user['username']] = user
        return list(all_users.values())

    def get_user_info_by_timeframe(self, username, timeframe='TOTAL'):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT MAX(timestamp) FROM snaps WHERE timeframe = ?", (timeframe,))
            latest_ts = cursor.fetchone()[0]
            if not latest_ts: return self.get_user_info(username)
            query = """
                SELECT username, displayName, rank, cSnapsPercentRank, 
                       snapsPercent, cSnapsPercent, followers, smartFollowers, 
                       profileImageUrl
                FROM snaps WHERE username = ? AND timeframe = ? AND timestamp = ?
            """
            user_df = pd.read_sql(query, conn, params=(username, timeframe, latest_ts))
            if not user_df.empty: return user_df.iloc[0].to_dict()
        return self.get_user_info(username)

    def get_user_info(self, username):
        query = "SELECT username, displayName, profileImageUrl, followers, smartFollowers FROM snaps WHERE username = ? ORDER BY timestamp DESC LIMIT 1"
        with sqlite3.connect(self.db_path) as conn:
            df = pd.read_sql(query, conn, params=(username,))
            if not df.empty: return df.iloc[0].to_dict()
        return {'username': username, 'displayName': username}

    def get_user_analysis(self, username):
        return {tf: self.get_user_history(username, tf) for tf in self.timeframes}

    def compare_leaderboards(self, timestamp1, timestamp2, timeframe='TOTAL', metric='snapsPercent'):
        # 1. 컬럼 설정
        if metric == 'snapsPercent':
            rank_col, ms_col, diff_col = 'rank', 'snapsPercent', 'mindshare_change'
        else:
            rank_col, ms_col, diff_col = 'cSnapsPercentRank', 'cSnapsPercent', 'c_mindshare_change'
            
        # 2. 데이터 가져오기
        df1 = self.get_leaderboard_at_timestamp(timestamp1, timeframe)
        df2 = self.get_leaderboard_at_timestamp(timestamp2, timeframe)
        if df1.empty and df2.empty: return pd.DataFrame()

        # 3. 데이터 전처리 (이름 변경)
        df1 = df1[['username', 'displayName', rank_col, ms_col, 'profileImageUrl']].rename(columns={
            rank_col: 'prev_rank', ms_col: 'prev_mindshare', 'profileImageUrl': 'prev_profileImageUrl'
        })
        df2 = df2[['username', 'displayName', rank_col, ms_col, 'profileImageUrl']].rename(columns={
            rank_col: 'curr_rank', ms_col: 'curr_mindshare', 'profileImageUrl': 'curr_profileImageUrl'
        })
        
        # 4. 병합 및 결측치 채우기
        compare_data = pd.merge(df1, df2, on='username', how='outer', suffixes=('_prev', '_curr'))
        compare_data['displayName'] = compare_data['displayName_curr'].combine_first(compare_data['displayName_prev']).fillna('')
        compare_data['profileImageUrl'] = compare_data['curr_profileImageUrl'].combine_first(compare_data['prev_profileImageUrl']).fillna('')
        
        # 여기서 9999와 0으로 채움
        compare_data.fillna({'prev_mindshare': 0, 'curr_mindshare': 0, 'prev_rank': 9999, 'curr_rank': 9999}, inplace=True)
        
        # 5. 변동폭 계산
        compare_data['rank_change'] = compare_data['prev_rank'] - compare_data['curr_rank']
        compare_data[diff_col] = compare_data['curr_mindshare'] - compare_data['prev_mindshare']
        
        # --- [수정된 부분] 보정 로직 시작 ---
        
        # (1) 순위 변동 보정: 500등 이상 차이나면(진입/이탈) 순위 변동 0 처리
        compare_data['rank_change'] = np.where(abs(compare_data['rank_change']) > 500, 0, compare_data['rank_change'])

        # (2) 마인드쉐어 변동 보정: 이전이나 현재 순위 중 하나라도 999(순위 밖)면 마인드쉐어 변동 0 처리
        compare_data[diff_col] = np.where(
            (compare_data['prev_rank'] == 9999) | (compare_data['curr_rank'] == 9999), 
            0, 
            compare_data[diff_col]
        )
        
        # --- [수정된 부분] 보정 로직 끝 ---

        # 6. 결과 정리
        result = compare_data[['username', 'displayName', 'profileImageUrl', 'prev_rank', 'curr_rank', 'rank_change', 
                              'prev_mindshare', 'curr_mindshare', diff_col]].copy()
        
        if metric == 'cSnapsPercent':
             result.rename(columns={'prev_mindshare': 'prev_c_mindshare', 'curr_mindshare': 'curr_c_mindshare'}, inplace=True)
        
        result.sort_values('rank_change', ascending=False, inplace=True)
        return result

    def get_all_users(self):
        return self.get_all_usernames('TOTAL')