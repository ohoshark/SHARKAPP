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
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # 유저 정보 테이블
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    infoName TEXT PRIMARY KEY,
                    displayName TEXT,
                    imageUrl TEXT,
                    wal_score INTEGER
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
    
    def update_user(self, info_name, display_name=None, image_url=None, wal_score=None):
        """유저 정보 업데이트 (wallchain 우선, 변경사항 있으면 갱신)"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # 기존 데이터 확인
            cursor.execute('SELECT displayName, imageUrl, wal_score FROM users WHERE infoName = ?', (info_name,))
            existing = cursor.fetchone()
            
            if existing:
                # 기존 데이터가 있으면 업데이트 (wallchain 데이터 우선)
                new_display_name = display_name if display_name else existing[0]
                new_image_url = image_url if image_url else existing[1]
                new_wal_score = wal_score if wal_score is not None else existing[2]
                
                cursor.execute('''
                    UPDATE users 
                    SET displayName = ?, imageUrl = ?, wal_score = ?
                    WHERE infoName = ?
                ''', (new_display_name, new_image_url, new_wal_score, info_name))
            else:
                # 새로운 유저 추가
                cursor.execute('''
                    INSERT INTO users (infoName, displayName, imageUrl, wal_score)
                    VALUES (?, ?, ?, ?)
                ''', (info_name, display_name, image_url, wal_score))
            
            conn.commit()
    
    def update_ranking(self, info_name, project_name, timeframe, ms_rank=None, 
                      cms_rank=None, ms=None, cms=None, 
                      position_change=None):
        """순위 정보 업데이트"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT OR REPLACE INTO rankings 
                (infoName, projectName, timeframe, msRank, cmsRank, ms, cms, positionChange)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (info_name, project_name, timeframe, ms_rank, cms_rank, 
                  ms, cms, position_change))
            
            conn.commit()
    
    def search_users(self, query, limit=10):
        """유저 검색 (infoName, displayName 모두 검색)"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            search_pattern = f'%{query}%'
            cursor.execute('''
                SELECT infoName, displayName, imageUrl, wal_score
                FROM users
                WHERE infoName LIKE ? OR displayName LIKE ?
                ORDER BY infoName
                LIMIT ?
            ''', (search_pattern, search_pattern, limit))
            
            results = cursor.fetchall()
            return [
                {
                    'infoName': row[0],
                    'displayName': row[1],
                    'imageUrl': row[2],
                    'wal_score': row[3]
                }
                for row in results
            ]
    
    def get_user_data(self, info_name):
        """특정 유저의 전체 데이터 가져오기"""
        with sqlite3.connect(self.db_path) as conn:
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
                'wal_score': user_row[3]
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
                
                # 프로젝트명으로 cookie/wallchain 구분
                if project_name.startswith('wallchain-'):
                    if project_name not in wallchain_projects:
                        wallchain_projects[project_name] = []
                    wallchain_projects[project_name].append(ranking_data)
                else:
                    if project_name not in cookie_projects:
                        cookie_projects[project_name] = []
                    cookie_projects[project_name].append(ranking_data)
            
            return {
                'user': user_info,
                'cookie_projects': cookie_projects,
                'wallchain_projects': wallchain_projects
            }
    
    def clear_all_rankings(self):
        """모든 순위 데이터 삭제 (갱신 전)"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('DELETE FROM rankings')
            conn.commit()
    
    def begin_batch_update(self):
        """배치 업데이트 시작 - 임시 테이블 생성"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # 임시 테이블 생성 (기존 테이블과 동일 구조)
            cursor.execute('DROP TABLE IF EXISTS users_temp')
            cursor.execute('DROP TABLE IF EXISTS rankings_temp')
            
            cursor.execute('''
                CREATE TABLE users_temp (
                    infoName TEXT PRIMARY KEY,
                    displayName TEXT,
                    imageUrl TEXT,
                    wal_score INTEGER
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
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.executemany('''
                INSERT OR REPLACE INTO users_temp (infoName, displayName, imageUrl, wal_score)
                VALUES (?, ?, ?, ?)
            ''', users_data)
            conn.commit()
    
    def batch_insert_rankings(self, rankings_data):
        """순위 데이터 배치 삽입"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.executemany('''
                INSERT OR REPLACE INTO rankings_temp 
                (infoName, projectName, timeframe, msRank, cmsRank, ms, cms, positionChange)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', rankings_data)
            conn.commit()
    
    def commit_batch_update(self):
        """배치 업데이트 완료 - 임시 테이블을 실제 테이블로 교체 (원자적)"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # 트랜잭션으로 한번에 교체
            cursor.execute('BEGIN IMMEDIATE')
            try:
                # 기존 테이블 삭제
                cursor.execute('DROP TABLE IF EXISTS users_old')
                cursor.execute('DROP TABLE IF EXISTS rankings_old')
                
                # 현재 테이블을 old로 변경
                cursor.execute('ALTER TABLE users RENAME TO users_old')
                cursor.execute('ALTER TABLE rankings RENAME TO rankings_old')
                
                # 임시 테이블을 실제 테이블로 변경
                cursor.execute('ALTER TABLE users_temp RENAME TO users')
                cursor.execute('ALTER TABLE rankings_temp RENAME TO rankings')
                
                # 인덱스 재생성
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_rankings_infoName ON rankings(infoName)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_users_displayName ON users(displayName)')
                
                # old 테이블 삭제
                cursor.execute('DROP TABLE users_old')
                cursor.execute('DROP TABLE rankings_old')
                
                conn.commit()
                print("[UnifiedDataManager] 배치 업데이트 완료 - 테이블 교체 성공")
            except Exception as e:
                conn.rollback()
                print(f"[UnifiedDataManager] 배치 업데이트 실패: {e}")
                raise
