import os
import orjson
import sqlite3
import json
import pandas as pd
import numpy as np
from datetime import datetime
import glob
from collections import defaultdict

class DataProcessorWallchain:
    def __init__(self, data_dir):
        self.data_dir = data_dir
        
        # 동적으로 timeframe 감지
        self.timeframes = self._detect_timeframes()
        
        # DB 파일 경로 설정
        self.db_path = os.path.join(data_dir, "wallchain_data.db")
        
        # 1. DB 초기화 (테이블 및 인덱스 생성)
        self._init_db()
        
        # 2. 최신 파일 정보 로드
        self.latest_file = self._load_latest_file_info()

    def _detect_timeframes(self):
        """data_dir 내의 실제 폴더를 스캔하여 timeframe 목록 생성"""
        timeframes = []
        if os.path.exists(self.data_dir):
            for item in os.listdir(self.data_dir):
                item_path = os.path.join(self.data_dir, item)
                # 폴더이고, 숨김 폴더가 아니며, .db 파일이 아닌 경우
                if os.path.isdir(item_path) and not item.startswith('_') and not item.startswith('.'):
                    # epoch_2 같은 언더스코어 형식을 하이픈으로 정규화
                    normalized = self.normalize_timeframe(item)
                    timeframes.append(normalized)
                    # print(f"[Wallchain] Detected timeframe: {item} -> {normalized}")
        
        result = sorted(timeframes) if timeframes else ['7d', '30d', 'epoch-2']
        # print(f"[Wallchain] Final timeframes for {self.data_dir}: {result}")
        return result

    def normalize_timeframe(self, timeframe):
        """timeframe 명칭을 정규화 (epoch_2 -> epoch-2, epoch_omega 유지)"""
        # epoch_숫자 형식만 하이픈으로 변경
        if timeframe.startswith('epoch_') and timeframe.split('_')[1].isdigit():
            return timeframe.replace('_', '-')
        return timeframe

    def _init_db(self):
        """DB 연결 및 필요한 테이블/인덱스 생성"""
        with sqlite3.connect(self.db_path, check_same_thread=False) as conn:
            cursor = conn.cursor()
            
            # WAL 모드 활성화 (쓰기 중에도 읽기 가능)
            cursor.execute('PRAGMA journal_mode=WAL')
            cursor.execute('PRAGMA busy_timeout=30000')  # 30초 타임아웃
            
            # 메인 데이터 테이블 생성 (wallchain 데이터 구조)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS leaderboard (
                    id TEXT,
                    name TEXT,
                    username TEXT,
                    imageUrl TEXT,
                    rank INTEGER,
                    score INTEGER,
                    scorePercentile REAL,
                    scoreQuantile REAL,
                    mindsharePercentage REAL,
                    relativeMindshare REAL,
                    appUseMultiplier REAL,
                    position INTEGER,
                    positionChange INTEGER,
                    timeframe TEXT,
                    timestamp TEXT
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
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_user_tf_wall ON leaderboard (username, timeframe)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_ts_tf_wall ON leaderboard (timestamp, timeframe)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_position ON leaderboard (position, timeframe, timestamp)")
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
                
                # timeframe을 정규화 (epoch_2가 키로 올 가능성 대비)
                normalized_tf = self.normalize_timeframe(timeframe)
                
                all_records = []
                for file_path in files:
                    try:
                        filename = os.path.basename(file_path)
                        parts = filename.replace('.json', '').split('_')
                        ts_str = f"{parts[0]}_{parts[1]}"
                        timestamp = datetime.strptime(ts_str, '%Y%m%d_%H%M%S').strftime('%Y-%m-%d %H:%M:%S')
                        
                        with open(file_path, 'rb') as f:
                            raw_data = orjson.loads(f.read())
                        
                        # wallchain 데이터 구조 처리
                        if isinstance(raw_data, list):
                            for page in raw_data:
                                if 'entries' in page:
                                    for entry in page['entries']:
                                        record = {}
                                        # xInfo 데이터 추출
                                        if 'xInfo' in entry:
                                            record.update(entry['xInfo'])
                                        # 나머지 필드 추가
                                        record['mindsharePercentage'] = entry.get('mindsharePercentage', 0)
                                        record['relativeMindshare'] = entry.get('relativeMindshare', 0)
                                        record['appUseMultiplier'] = entry.get('appUseMultiplier', 1.0)
                                        record['position'] = entry.get('position', 0)
                                        record['positionChange'] = entry.get('positionChange', 0)
                                        # 정규화된 timeframe 사용 (epoch_2 -> epoch-2)
                                        record['timeframe'] = normalized_tf
                                        record['timestamp'] = timestamp
                                        all_records.append(record)
                            
                            # 최신 파일 정보를 정규화된 timeframe으로 갱신
                            self.latest_file[normalized_tf] = filename
                            self._save_latest_file_info(normalized_tf, filename)
                            new_data_found = True
                    except Exception as e:
                        print(f"Error parsing {file_path}: {e}")

                if all_records:
                    df = pd.DataFrame(all_records)
                    
                    # DB 스키마 자동 업데이트
                    cursor = conn.cursor()
                    cursor.execute("PRAGMA table_info(leaderboard)")
                    existing_columns = [info[1] for info in cursor.fetchall()]
                    for col in df.columns:
                        if col not in existing_columns:
                            cursor.execute(f"ALTER TABLE leaderboard ADD COLUMN {col} TEXT")
                    
                    df.to_sql('leaderboard', conn, if_exists='append', index=False)
                    print(f"[Wallchain - {normalized_tf}] DB Insert Complete.")

        # 데이터 삽입이 완전히 끝난 후 파일 정리 실행
        if new_data_found:
            self.cleanup_old_files()
            
        return new_data_found

    def cleanup_old_files(self):
        """DB에 기록된 최신 파일보다 '과거'의 파일들만 삭제합니다."""
        print(f"--- [Wallchain - {self.data_dir}] 안전한 파일 정리 시작 ---")
        
        for tf in self.timeframes:
            # 원본 폴더명 찾기 (정규화 전)
            original_folder = None
            for item in os.listdir(self.data_dir):
                item_path = os.path.join(self.data_dir, item)
                if os.path.isdir(item_path) and self.normalize_timeframe(item) == tf:
                    original_folder = item
                    break
            
            if not original_folder:
                continue
            
            path = os.path.join(self.data_dir, original_folder)
            if not os.path.exists(path):
                continue
            
            # DB가 기억하는 이 타임프레임의 최신 파일명 (기준점)
            latest_filename = self.latest_file.get(tf, "")
            if not latest_filename:
                continue
            
            # 폴더 내 모든 json 파일 리스트업
            all_files = glob.glob(os.path.join(path, "*.json"))
            
            for f_path in all_files:
                f_name = os.path.basename(f_path)
                
                # '기준이 되는 최신 파일'보다 이름(시간)이 작은 파일만 삭제
                if f_name < latest_filename:
                    try:
                        os.remove(f_path)
                        print(f"Deleted old file: {f_name} from {original_folder}")
                    except Exception as e:
                            print(f"Failed to delete {f_name}: {e}")

    def check_for_new_data(self):
        """새로 생성된 JSON 파일이 있는지 체크합니다."""
        new_files = defaultdict(list)
        any_new = False
        
        # 실제 폴더를 다시 스캔하여 새로운 timeframe도 감지
        current_timeframes = self._detect_timeframes()
        if current_timeframes != self.timeframes:
            # print(f"[Wallchain] 새로운 timeframe 감지: {set(current_timeframes) - set(self.timeframes)}")
            self.timeframes = current_timeframes
        
        for tf in self.timeframes:
            # 원본 폴더명 찾기 (정규화 전)
            original_folder = None
            for item in os.listdir(self.data_dir):
                item_path = os.path.join(self.data_dir, item)
                if os.path.isdir(item_path) and self.normalize_timeframe(item) == tf:
                    original_folder = item
                    break
            
            if not original_folder:
                continue
            
            path = os.path.join(self.data_dir, original_folder)
            if not os.path.exists(path):
                continue
            
            all_files = sorted(glob.glob(os.path.join(path, "*.json")))
            last_loaded = self.latest_file.get(tf, "")
            for f in all_files:
                if os.path.basename(f) > last_loaded:
                    new_files[tf].append(f)
                    any_new = True
        return new_files if any_new else {}

    # --- 데이터 조회 함수들 ---

    def get_available_timestamps(self, timeframe='epoch-2'):
        query = "SELECT DISTINCT timestamp FROM leaderboard WHERE timeframe = ? ORDER BY timestamp ASC"
        with sqlite3.connect(self.db_path) as conn:
            df = pd.read_sql(query, conn, params=(timeframe,))
        return df['timestamp'].tolist()

    def get_leaderboard_at_timestamp(self, timestamp, timeframe='epoch-2'):
        query = """
            SELECT username, name, position, positionChange,
                   mindsharePercentage, relativeMindshare, 
                   rank, score, scorePercentile, appUseMultiplier,
                   imageUrl, timestamp, timeframe 
            FROM leaderboard WHERE timestamp = ? AND timeframe = ?
            ORDER BY position ASC
        """
        with sqlite3.connect(self.db_path) as conn:
            return pd.read_sql(query, conn, params=(timestamp, timeframe))

    def get_user_history(self, username, timeframe='epoch-2'):
        query = """
            SELECT name, timestamp, position, positionChange,
                   mindsharePercentage, rank, score
            FROM leaderboard WHERE username = ? AND timeframe = ? ORDER BY timestamp ASC
        """
        with sqlite3.connect(self.db_path) as conn:
            history = pd.read_sql(query, conn, params=(username, timeframe))
        if history.empty: return pd.DataFrame()
        history['timestamp'] = pd.to_datetime(history['timestamp'])
        if len(history) > 500:
            indices = np.linspace(0, len(history) - 1, 500).astype(int)
            history = history.iloc[indices]
        return history

    def get_all_usernames(self, timeframe='epoch-2'):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT MAX(timestamp) FROM leaderboard WHERE timeframe = ?", (timeframe,))
            latest_ts = cursor.fetchone()[0]
            if not latest_ts: return []
            query = "SELECT username, name FROM leaderboard WHERE timestamp = ? AND timeframe = ? ORDER BY position ASC"
            return pd.read_sql(query, conn, params=(latest_ts, timeframe)).to_dict('records')
    
    def get_all_usernames_from_all_timeframes(self):
        """모든 timeframe에서 사용자를 가져와 중복 제거 후 반환"""
        all_users = {}
        with sqlite3.connect(self.db_path) as conn:
            for tf in self.timeframes:
                cursor = conn.cursor()
                cursor.execute("SELECT MAX(timestamp) FROM leaderboard WHERE timeframe = ?", (tf,))
                latest_ts = cursor.fetchone()[0]
                if not latest_ts:
                    continue
                query = "SELECT username, name FROM leaderboard WHERE timestamp = ? AND timeframe = ? ORDER BY position ASC"
                users = pd.read_sql(query, conn, params=(latest_ts, tf)).to_dict('records')
                for user in users:
                    # username을 키로 사용하여 중복 제거
                    if user['username'] not in all_users:
                        all_users[user['username']] = user
        return list(all_users.values())

    def get_user_info_by_timeframe(self, username, timeframe='epoch-2'):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT MAX(timestamp) FROM leaderboard WHERE timeframe = ?", (timeframe,))
            latest_ts = cursor.fetchone()[0]
            if not latest_ts: return self.get_user_info(username)
            query = """
                SELECT username, name, position, positionChange,
                       mindsharePercentage, relativeMindshare,
                       rank, score, scorePercentile, appUseMultiplier, imageUrl
                FROM leaderboard WHERE username = ? AND timeframe = ? AND timestamp = ?
            """
            user_df = pd.read_sql(query, conn, params=(username, timeframe, latest_ts))
            if not user_df.empty: return user_df.iloc[0].to_dict()
        return self.get_user_info(username)

    def get_user_info(self, username):
        query = "SELECT username, name, imageUrl, rank, score FROM leaderboard WHERE username = ? ORDER BY timestamp DESC LIMIT 1"
        with sqlite3.connect(self.db_path) as conn:
            df = pd.read_sql(query, conn, params=(username,))
            if not df.empty: return df.iloc[0].to_dict()
        return {'username': username, 'name': username}

    def get_user_analysis(self, username):
        return {tf: self.get_user_history(username, tf) for tf in self.timeframes}

    def compare_leaderboards(self, timestamp1, timestamp2, timeframe='epoch-2'):
        df1 = self.get_leaderboard_at_timestamp(timestamp1, timeframe)
        df2 = self.get_leaderboard_at_timestamp(timestamp2, timeframe)
        if df1.empty and df2.empty: return pd.DataFrame()

        # 필요한 컬럼만 선택하고 rename
        df1 = df1[['username', 'name', 'position', 'mindsharePercentage', 'imageUrl']].rename(columns={
            'position': 'prev_position', 'mindsharePercentage': 'prev_mindshare', 'imageUrl': 'prev_imageUrl'
        })
        df2 = df2[['username', 'name', 'position', 'mindsharePercentage', 'imageUrl']].rename(columns={
            'position': 'curr_position', 'mindsharePercentage': 'curr_mindshare', 'imageUrl': 'curr_imageUrl'
        })
        
        # outer join으로 병합
        compare_data = pd.merge(df1, df2, on='username', how='outer', suffixes=('_prev', '_curr'))
        
        # name과 imageUrl 병합 (빈 문자열이 아닌 값 우선)
        compare_data['name'] = compare_data['name_curr'].fillna(compare_data['name_prev']).fillna('')
        compare_data['imageUrl'] = compare_data['curr_imageUrl'].fillna(compare_data['prev_imageUrl']).fillna('')
        
        # 결측값 처리 (순위 밖은 9999로 설정)
        compare_data['prev_mindshare'] = compare_data['prev_mindshare'].fillna(0)
        compare_data['curr_mindshare'] = compare_data['curr_mindshare'].fillna(0)
        compare_data['prev_position'] = compare_data['prev_position'].fillna(9999)
        compare_data['curr_position'] = compare_data['curr_position'].fillna(9999)
        
        # 순위 변화 및 마인드쉐어 변화 기본 계산
        compare_data['position_change'] = compare_data['prev_position'] - compare_data['curr_position']
        compare_data['mindshare_change'] = compare_data['curr_mindshare'] - compare_data['prev_mindshare']
        
        # --- [보정 로직 시작] ---
        
        # 1. 비정상적인 순위 변화 처리 (500위 이상 변동 시 0 처리)
        compare_data['position_change'] = np.where(
            abs(compare_data['position_change']) > 500, 
            0, 
            compare_data['position_change']
        )

        # 2. [추가됨] 마인드쉐어 변동 보정 (순위 밖(9999)에서 들어오거나 나갈 때 0 처리)
        compare_data['mindshare_change'] = np.where(
            (compare_data['prev_position'] == 9999) | (compare_data['curr_position'] == 9999),
            0,
            compare_data['mindshare_change']
        )
        
        # --- [보정 로직 끝] ---

        result = compare_data[['username', 'name', 'imageUrl', 'prev_position', 'curr_position', 'position_change', 
                               'prev_mindshare', 'curr_mindshare', 'mindshare_change']].copy()
        
        # 현재 순위 기준으로 정렬 (9999는 맨 뒤로)
        result.sort_values(['curr_position', 'prev_position'], ascending=[True, True], inplace=True)
        return result

    def get_all_users(self):
        return self.get_all_usernames('epoch-2')
