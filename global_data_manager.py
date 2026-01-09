import sqlite3
import os
from datetime import datetime
import logging

class GlobalDataManager:
    def __init__(self, db_path='./data/global_rankings.db'):
        self.db_path = db_path
        self.init_database()
    
    def init_database(self):
        """글로벌 DB 초기화 및 테이블 생성"""
        with sqlite3.connect(self.db_path, timeout=30.0) as conn:
            cursor = conn.cursor()
            
            # WAL 모드 활성화 (동시 읽기/쓰기 지원)
            cursor.execute('PRAGMA journal_mode=WAL')
            cursor.execute('PRAGMA synchronous=NORMAL')
            
            # 유저 정보 테이블
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    infoName TEXT PRIMARY KEY,
                    displayName TEXT,
                    imageUrl TEXT,
                    wal_score INTEGER,
                    cookie_smart_follower INTEGER,
                    kaito_smart_follower INTEGER,
                    follower INTEGER
                )
            ''')
                      
            # 순위 정보 테이블
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS rankings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    infoName TEXT,
                    projectName TEXT,
                    timeframe TEXT,
                    msRank INTEGER,
                    cmsRank INTEGER,
                    ms REAL,
                    cms REAL,
                    positionChange INTEGER,
                    UNIQUE(infoName, projectName, timeframe)
                )
            ''')
            
            # 인덱스 생성
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_rankings_infoName ON rankings(infoName)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_users_displayName ON users(displayName)')
            
            conn.commit()
            print("[GlobalDataManager] Database initialized")
    
    def update_user(self, info_name, display_name=None, image_url=None, wal_score=None, 
                   cookie_smart_follower=None, kaito_smart_follower=None, follower=None):
        """유저 정보 업데이트 (wallchain > cookie > kaito 우선순위)"""
        with sqlite3.connect(self.db_path, timeout=30.0) as conn:
            cursor = conn.cursor()
            
            # 기존 데이터 확인
            cursor.execute('SELECT displayName, imageUrl, wal_score, cookie_smart_follower, kaito_smart_follower, follower FROM users WHERE infoName = ?', (info_name,))
            existing = cursor.fetchone()
            
            if existing:
                # 기존 데이터가 있으면 업데이트
                new_display_name = display_name if display_name else existing[0]
                new_wal_score = wal_score if wal_score is not None else existing[2]
                
                # image_url 우선순위 처리: wallchain > cookie > kaito
                # kaito는 숫자만 있는 경우가 많으므로, 숫자만 있으면 기존 데이터 유지
                if image_url:
                    # 새로운 image_url이 숫자만 있는 경우 (kaito)
                    if image_url.isdigit():
                        # 기존에 숫자가 아닌 URL이 있으면 유지 (wallchain/cookie 우선)
                        new_image_url = existing[1] if existing[1] and not existing[1].isdigit() else image_url
                    else:
                        # 숫자가 아닌 URL (wallchain/cookie)은 항상 업데이트
                        new_image_url = image_url
                else:
                    # image_url이 None이면 기존 데이터 유지
                    new_image_url = existing[1]
                
                # 팔로워 정보 업데이트 (값이 있을 때만)
                new_cookie_smart = cookie_smart_follower if cookie_smart_follower is not None else existing[3]
                new_kaito_smart = kaito_smart_follower if kaito_smart_follower is not None else existing[4]
                new_follower = follower if follower is not None else existing[5]
                
                cursor.execute('''
                    UPDATE users 
                    SET displayName = ?, imageUrl = ?, wal_score = ?,
                        cookie_smart_follower = ?, kaito_smart_follower = ?, follower = ?
                    WHERE infoName = ?
                ''', (new_display_name, new_image_url, new_wal_score,
                      new_cookie_smart, new_kaito_smart, new_follower, info_name))
            else:
                # 새로운 유저 추가
                cursor.execute('''
                    INSERT INTO users (infoName, displayName, imageUrl, wal_score,
                                     cookie_smart_follower, kaito_smart_follower, follower)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (info_name, display_name, image_url, wal_score,
                      cookie_smart_follower, kaito_smart_follower, follower))
            
            conn.commit()            # WAL 체크포인트 실행 - 변경사항을 메인 DB에 즉시 반영
            cursor.execute('PRAGMA wal_checkpoint(PASSIVE)')    
    def update_ranking(self, info_name, project_name, timeframe, ms_rank=None, 
                      cms_rank=None, ms=None, cms=None, 
                      position_change=None):
        """순위 정보 업데이트"""
        with sqlite3.connect(self.db_path, timeout=30.0) as conn:
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT OR REPLACE INTO rankings 
                (infoName, projectName, timeframe, msRank, cmsRank, ms, cms, positionChange)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (info_name, project_name, timeframe, ms_rank, cms_rank, 
                  ms, cms, position_change))
            
            conn.commit()
            # WAL 체크포인트 실행 - 변경사항을 메인 DB에 즉시 반영
            cursor.execute('PRAGMA wal_checkpoint(PASSIVE)')
    
    def search_users(self, query, limit=10):
        """유저 검색 (infoName, displayName 모두 검색) - SQLite 쿼리 기반 (한글 완벽 지원)"""
        with sqlite3.connect(self.db_path, timeout=30.0) as conn:
            cursor = conn.cursor()
            
            # @ prefix 제거
            if query.startswith('@'):
                query = query[1:]
            
            search_pattern = f'%{query}%'
            
            # SQLite에서 직접 필터링 (인덱스 활용, 효율적)
            cursor.execute('''
                SELECT infoName, displayName, imageUrl, wal_score,
                       cookie_smart_follower, kaito_smart_follower, follower
                FROM users
                WHERE infoName LIKE ? COLLATE NOCASE 
                   OR displayName LIKE ?
                ORDER BY 
                    CASE WHEN infoName LIKE ? COLLATE NOCASE THEN 0 ELSE 1 END,
                    infoName
                LIMIT ?
            ''', (search_pattern, search_pattern, search_pattern, limit))
            
            results = cursor.fetchall()
            return [
                {
                    'infoName': row[0],
                    'displayName': row[1],
                    'imageUrl': row[2],
                    'wal_score': row[3],
                    'cookie_smart_follower': row[4],
                    'kaito_smart_follower': row[5],
                    'follower': row[6]
                }
                for row in results
            ]
    
    def get_user_data(self, info_name):
        """특정 유저의 전체 데이터 가져오기"""
        with sqlite3.connect(self.db_path, timeout=30.0) as conn:
            cursor = conn.cursor()
            
            # 유저 기본 정보
            cursor.execute('SELECT * FROM users WHERE infoName = ?', (info_name,))
            user_row = cursor.fetchone()
            
            if not user_row:
                return None
            
            user_info = {
                'infoName': user_row[0],
                'displayName': user_row[1],
                'imageUrl': user_row[2],
                'wal_score': user_row[3],
                'cookie_smart_follower': user_row[4] if len(user_row) > 4 else None,
                'kaito_smart_follower': user_row[5] if len(user_row) > 5 else None,
                'follower': user_row[6] if len(user_row) > 6 else None
            }
            
            # 순위 정보 (프로젝트별로 그룹화)
            cursor.execute('''
                SELECT projectName, timeframe, msRank, cmsRank, ms, cms, positionChange
                FROM rankings
                WHERE infoName = ?
                ORDER BY projectName, timeframe
            ''', (info_name,))
            
            rankings_rows = cursor.fetchall()
            
            # 프로젝트별로 그룹화
            cookie_projects = {}
            wallchain_projects = {}
            kaito_projects = {}
            
            for row in rankings_rows:
                project_name = row[0]
                timeframe = row[1]
                ms_rank = row[2]
                cms_rank = row[3]
                ms = row[4]
                cms = row[5]
                position_change = row[6]
                
                ranking_data = {
                    'timeframe': timeframe,
                    'msRank': ms_rank,
                    'cmsRank': cms_rank,
                    'ms': ms,
                    'cms': cms,
                    'positionChange': position_change
                }
                
                # 프로젝트명으로 cookie/wallchain/kaito 구분
                if project_name.startswith('wallchain-'):
                    if project_name not in wallchain_projects:
                        wallchain_projects[project_name] = []
                    wallchain_projects[project_name].append(ranking_data)
                elif project_name.startswith('kaito-'):
                    if project_name not in kaito_projects:
                        kaito_projects[project_name] = []
                    kaito_projects[project_name].append(ranking_data)
                else:
                    if project_name not in cookie_projects:
                        cookie_projects[project_name] = []
                    cookie_projects[project_name].append(ranking_data)
            
            return {
                'user': user_info,
                'cookie_projects': cookie_projects,
                'wallchain_projects': wallchain_projects,
                'kaito_projects': kaito_projects
            }
    
    def clear_all_rankings(self):
        """모든 순위 데이터 삭제 (갱신 전)"""
        with sqlite3.connect(self.db_path, timeout=30.0) as conn:
            cursor = conn.cursor()
            cursor.execute('DELETE FROM rankings')
            conn.commit()
    
    def begin_batch_update(self):
        """배치 업데이트 시작 - 임시 테이블 생성"""
        with sqlite3.connect(self.db_path, timeout=30.0) as conn:
            cursor = conn.cursor()
            
            # 임시 테이블 생성 (기존 테이블과 동일 구조)
            cursor.execute('DROP TABLE IF EXISTS users_temp')
            cursor.execute('DROP TABLE IF EXISTS rankings_temp')
            
            cursor.execute('''
                CREATE TABLE users_temp (
                    infoName TEXT PRIMARY KEY,
                    displayName TEXT,
                    imageUrl TEXT,
                    wal_score INTEGER,
                    cookie_smart_follower INTEGER,
                    kaito_smart_follower INTEGER,
                    follower INTEGER
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE rankings_temp (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    infoName TEXT,
                    projectName TEXT,
                    timeframe TEXT,
                    msRank INTEGER,
                    cmsRank INTEGER,
                    ms REAL,
                    cms REAL,
                    positionChange INTEGER,
                    UNIQUE(infoName, projectName, timeframe)
                )
            ''')
            
            conn.commit()
    
    def batch_insert_users(self, users_data):
        """유저 데이터 배치 삽입"""
        with sqlite3.connect(self.db_path, timeout=30.0) as conn:
            cursor = conn.cursor()
            cursor.executemany('''
                INSERT OR REPLACE INTO users_temp (infoName, displayName, imageUrl, wal_score,
                                                  cookie_smart_follower, kaito_smart_follower, follower)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', users_data)
            conn.commit()
    
    def batch_insert_rankings(self, rankings_data):
        """순위 데이터 배치 삽입"""
        with sqlite3.connect(self.db_path, timeout=30.0) as conn:
            cursor = conn.cursor()
            cursor.executemany('''
                INSERT OR REPLACE INTO rankings_temp 
                (infoName, projectName, timeframe, msRank, cmsRank, ms, cms, positionChange)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', rankings_data)
            conn.commit()
    
    def commit_batch_update(self):
        """배치 업데이트 완료 - 임시 테이블을 실제 테이블로 교체 (원자적)"""
        with sqlite3.connect(self.db_path, timeout=30.0) as conn:
            cursor = conn.cursor()
            
            # 트랜잭션으로 한번에 교체
            cursor.execute('BEGIN IMMEDIATE')
            try:
                # 기존 old 테이블 삭제 (이전 실패 시 남아있을 수 있음)
                cursor.execute('DROP TABLE IF EXISTS users_old')
                cursor.execute('DROP TABLE IF EXISTS rankings_old')
                
                # 현재 테이블이 존재하는지 확인
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='users'")
                users_exists = cursor.fetchone() is not None
                
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='rankings'")
                rankings_exists = cursor.fetchone() is not None
                
                # 현재 테이블이 있으면 old로 변경, 없으면 스킵
                if users_exists:
                    cursor.execute('ALTER TABLE users RENAME TO users_old')
                if rankings_exists:
                    cursor.execute('ALTER TABLE rankings RENAME TO rankings_old')
                
                # 임시 테이블을 실제 테이블로 변경
                cursor.execute('ALTER TABLE users_temp RENAME TO users')
                cursor.execute('ALTER TABLE rankings_temp RENAME TO rankings')
                
                # 인덱스 재생성
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_rankings_infoName ON rankings(infoName)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_users_displayName ON users(displayName)')
                
                # old 테이블 삭제 (있는 경우에만)
                if users_exists:
                    cursor.execute('DROP TABLE users_old')
                if rankings_exists:
                    cursor.execute('DROP TABLE rankings_old')
                
                conn.commit()
                # WAL 체크포인트 실행 - 변경사항을 메인 DB에 즉시 반영
                cursor.execute('PRAGMA wal_checkpoint(PASSIVE)')
                print("[UnifiedDataManager] 배치 업데이트 완료 - 테이블 교체 성공")
            except Exception as e:
                conn.rollback()
                print(f"[UnifiedDataManager] 배치 업데이트 실패: {e}")
                raise
