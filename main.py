from bottle import Bottle, route, run, template, static_file, request, redirect, response, abort, TEMPLATE_PATH
from concurrent.futures import ThreadPoolExecutor  # 상단에 추가
import os
import json
import sqlite3
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.io as pio
import threading
import time
from datetime import datetime
from data_processor import DataProcessor
from data_processor_wallchain import DataProcessorWallchain
from data_processor_kaito import DataProcessorKaito
from global_data_manager import GlobalDataManager
import schedule

app = Bottle()

# 템플릿 경로 설정 (views 폴더와 루트 폴더 모두 포함)
TEMPLATE_PATH.insert(0, './views/')
TEMPLATE_PATH.insert(0, './')

base_data_dir = './data/cookie/'  # Cookie 데이터 디렉토리
base_wallchain_dir = './data/wallchain/'  # Wallchain 데이터 디렉토리
base_kaito_dir = './data/kaito/'  # Kaito 데이터 디렉토리

# 프로젝트별 DataProcessor 인스턴스 관리
project_instances = {}  # Cookie 프로젝트
wallchain_instances = {}  # Wallchain 프로젝트
kaito_processor = None  # Kaito 통합 프로세서
# main.py 파일 상단에 로그 파일 경로 설정
LOG_FILE = 'access_log.txt'

# 글로벌 데이터 관리자 초기화
global_manager = GlobalDataManager()

# main.py 파일 내 log_access 함수를 아래와 같이 수정
PROJECT_CACHE = {"list": [], "grouped": {}, "last_updated": 0}
WALLCHAIN_CACHE = {"list": [], "grouped": {}, "last_updated": 0}
KAITO_CACHE = {"list": [], "last_updated": 0}
CACHE_INTERVAL = 300  # 5분마다 갱신 (필요에 따라 조절)

# 로그 버퍼 (메모리에 쌓아두고 주기적으로 쓰기)
LOG_BUFFER = []
LOG_BUFFER_SIZE = 100  # 100개 쌓이면 파일에 쓰기
LOG_LOCK = threading.Lock()

def flush_logs():
    """버퍼에 쌓인 로그를 파일에 쓰기"""
    with LOG_LOCK:
        if LOG_BUFFER:
            try:
                with open(LOG_FILE, 'a', encoding='utf-8') as f:
                    f.writelines(LOG_BUFFER)
                LOG_BUFFER.clear()
            except Exception as e:
                print(f"[ERROR] 로그 파일 쓰기 실패: {e}")

def log_access(route_name, project_name, username=None):
    """
    접속 정보를 로그 파일에 기록합니다.
    (헤더: Cloudflare > X-Forwarded-For > X-Real-IP > REMOTE_ADDR 순으로 IP 확인)
    """
    
    # 1. Cloudflare 사용 시 헤더 (HTTP_CF_CONNECTING_IP)를 최우선으로 확인합니다.
    ip_address = request.environ.get('HTTP_CF_CONNECTING_IP')
    
    # 2. X-Forwarded-For 헤더 확인
    if not ip_address:
        x_forwarded_for = request.environ.get('HTTP_X_FORWARDED_FOR')
        # X-Forwarded-For는 여러 프록시를 거쳤을 경우 콤마로 구분된 리스트일 수 있으므로 가장 앞의 IP를 사용
        if x_forwarded_for:
            ip_address = x_forwarded_for.split(',')[0].strip()

    # 3. X-Real-IP 헤더 확인
    if not ip_address:
        ip_address = request.environ.get('HTTP_X_REAL_IP')

    # 4. 최후의 수단으로 REMOTE_ADDR (이것이 127.0.0.1이 됩니다.)
    if not ip_address:
        ip_address = request.environ.get('REMOTE_ADDR', 'UNKNOWN_IP')
    
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    user_agent = request.environ.get('HTTP_USER_AGENT', 'Unknown')
    session_id = f"{ip_address}_{user_agent}" 

    # 로그 메시지 포맷: 시간 | IP | 라우트 이름 | 프로젝트 | 사용자명 | 세션 ID
    log_message = f"{timestamp}|{ip_address}|{route_name}|{project_name}|{username or '-'}|{session_id}\n"
    
    # 버퍼에 추가
    with LOG_LOCK:
        LOG_BUFFER.append(log_message)
        # 버퍼가 꽉 차면 즉시 쓰기
        if len(LOG_BUFFER) >= LOG_BUFFER_SIZE:
            threading.Thread(target=flush_logs, daemon=True).start()
        
def get_cached_projects():
    current_time = time.time()
    
    # 프로젝트가 등록되지 않았다면 실시간으로 반환
    if not project_instances:
        return []
    
    # 마지막 업데이트로부터 5분이 지나지 않았으면 저장된 리스트 반환
    if PROJECT_CACHE["list"] and (current_time - PROJECT_CACHE["last_updated"] < CACHE_INTERVAL):
        return PROJECT_CACHE["list"]
    
    # 5분이 지났거나 리스트가 없으면 새로 스캔
    projects = sorted(project_instances.keys())
    PROJECT_CACHE["list"] = projects
    PROJECT_CACHE["last_updated"] = current_time
    return projects

def get_cached_wallchain_projects():
    current_time = time.time()
    
    if not wallchain_instances:
        return []
    
    if WALLCHAIN_CACHE["list"] and (current_time - WALLCHAIN_CACHE["last_updated"] < CACHE_INTERVAL):
        return WALLCHAIN_CACHE["list"]
    
    # wallchain- 접두사를 제거하고 프로젝트 이름만 반환
    projects = sorted([key.replace('wallchain-', '') for key in wallchain_instances.keys()])
    WALLCHAIN_CACHE["list"] = projects
    WALLCHAIN_CACHE["last_updated"] = current_time
    return projects

def get_grouped_projects():
    """Cookie 프로젝트를 이름별로 그룹화하여 반환"""
    current_time = time.time()
    
    if not project_instances:
        return {}
    
    if PROJECT_CACHE["grouped"] and (current_time - PROJECT_CACHE["last_updated"] < CACHE_INTERVAL):
        return PROJECT_CACHE["grouped"]
    
    grouped = {}
    for p in sorted(project_instances.keys()):
        parts = p.rsplit('-', 1)
        name = parts[0]
        lang = parts[1] if len(parts) > 1 else 'global'
        if name not in grouped:
            grouped[name] = []
        grouped[name].append({'full': p, 'lang': lang})
    
    PROJECT_CACHE["grouped"] = grouped
    return grouped

def get_grouped_wallchain_projects():
    """Wallchain 프로젝트를 이름별로 그룹화하여 반환"""
    current_time = time.time()
    
    if not wallchain_instances:
        return {}
    
    if WALLCHAIN_CACHE["grouped"] and (current_time - WALLCHAIN_CACHE["last_updated"] < CACHE_INTERVAL):
        return WALLCHAIN_CACHE["grouped"]
    
    grouped = {}
    for key in sorted(wallchain_instances.keys()):
        p = key.replace('wallchain-', '')
        parts = p.rsplit('-', 1)
        name = parts[0]
        lang = parts[1] if len(parts) > 1 else 'global'
        if name not in grouped:
            grouped[name] = []
        grouped[name].append({'full': p, 'lang': lang})
    
    WALLCHAIN_CACHE["grouped"] = grouped
    return grouped
        
def get_data_processor(project_name):
    # 등록된 인스턴스가 있는지 확인 (없으면 에러)
    if project_name not in project_instances:
        raise ValueError(f"Project '{project_name}' not found or not registered.")
    
    return project_instances[project_name]
def start_data_loader_thread(project_name):
    def project_periodic_loader():
        processor = project_instances[project_name]
        
        # 최초 실행 시 모든 데이터 로드
        try:
            print(f"[{project_name}] 초기 데이터 로드 시작...")
            processor.load_data()
            print(f"[{project_name}] ✅ 초기 데이터 로드 완료")
        except Exception as e:
            print(f"[{project_name}] ❌ 초기 데이터 로드 오류: {e}")
        
        # 주기적으로 신규 파일 체크
        while True:
            try:
                time.sleep(30)
                new_files = processor.check_for_new_data()
                if new_files:
                    print(f"[{project_name}] 신규 데이터 발견, 로드 중...")
                    processor.load_data(files_to_load=new_files)
                    print(f"[{project_name}] ✅ 신규 데이터 로드 완료")
            except Exception as e:
                print(f"[{project_name}] 데이터 로드 오류: {e}")

    thread = threading.Thread(target=project_periodic_loader, daemon=True)
    thread.start()
    print(f"[{project_name}] 데이터 로더 스레드 시작")

def init_projects_on_startup():
    if not os.path.exists(base_data_dir):
        os.makedirs(base_data_dir)
    
    project_instances.clear()

    for project_name in os.listdir(base_data_dir):
        project_path = os.path.join(base_data_dir, project_name)
        if not os.path.isdir(project_path) or project_name.startswith('_'):
            continue
            
        for lang in os.listdir(project_path):
            lang_path = os.path.join(project_path, lang)
            
            if os.path.isdir(lang_path) and not lang.startswith('_'):
                project_id = f"{project_name}-{lang}" 
                friendly_name = f"{project_name} ({lang.upper()})"
                
                # 1. DataProcessor 생성 (내부에서 DB 연결 및 테이블 생성됨)
                dp = DataProcessor(lang_path)
                
                # 2. 초기 데이터 로드는 백그라운드 스레드에서 처리
                # (웹서버를 먼저 시작하고 데이터는 나중에 로드)
                
                dp.project_display_title = friendly_name 
                dp.project_name = f"{project_name}"
                dp.lang = f"{lang}"
                
                project_instances[project_id] = dp
                
                # 3. 백그라운드 스레드 시작 (초기 데이터 로드 + 주기적으로 신규 파일 체크)
                start_data_loader_thread(project_id)
                print(f"🚀 Registered: {project_id} as '{friendly_name}' (데이터 로드 중...)")

def start_wallchain_loader_thread(project_name):
    def wallchain_periodic_loader():
        processor = wallchain_instances[project_name]
        
        # 최초 실행 시 모든 데이터 로드
        try:
            print(f"[Wallchain - {project_name}] 초기 데이터 로드 시작...")
            processor.load_data()
            print(f"[Wallchain - {project_name}] ✅ 초기 데이터 로드 완료")
        except Exception as e:
            print(f"[Wallchain - {project_name}] ❌ 초기 데이터 로드 오류: {e}")
        
        # 주기적으로 신규 파일 체크
        while True:
            try:
                time.sleep(30)
                new_files = processor.check_for_new_data()
                if new_files:
                    print(f"[Wallchain - {project_name}] 신규 데이터 발견, 로드 중...")
                    processor.load_data(files_to_load=new_files)
                    print(f"[Wallchain - {project_name}] ✅ 신규 데이터 로드 완료")
            except Exception as e:
                print(f"[Wallchain - {project_name}] 데이터 로드 오류: {e}")

    thread = threading.Thread(target=wallchain_periodic_loader, daemon=True)
    thread.start()
    print(f"[Wallchain - {project_name}] 데이터 로더 스레드 시작")

def init_wallchain_on_startup():
    if not os.path.exists(base_wallchain_dir):
        os.makedirs(base_wallchain_dir)
    
    wallchain_instances.clear()

    for project_name in os.listdir(base_wallchain_dir):
        project_path = os.path.join(base_wallchain_dir, project_name)
        if not os.path.isdir(project_path) or project_name.startswith('_') or project_name.startswith('.'):
            continue
            
        # wallchain은 언어 구분 없이 global 폴더 하위에 timeframe이 있음
        global_path = os.path.join(project_path, 'global')
        if os.path.isdir(global_path):
            project_id = f"wallchain-{project_name}"
            friendly_name = f"Wallchain: {project_name.upper()}"
            
            # DataProcessorWallchain 생성
            dp = DataProcessorWallchain(global_path)
            
            dp.project_display_title = friendly_name 
            dp.project_name = f"{project_name}"
            
            wallchain_instances[project_id] = dp
            
            # 백그라운드 스레드 시작
            start_wallchain_loader_thread(project_id)
            print(f"🌊 Registered: {project_id} as '{friendly_name}' (데이터 로드 중...)")

def scan_for_new_projects():
    """주기적으로 새로운 프로젝트를 스캔하여 등록"""
    def periodic_scanner():
        while True:
            try:
                time.sleep(300)  # 5분마다 스캔
                
                # Cookie 프로젝트 스캔
                if os.path.exists(base_data_dir):
                    for project_name in os.listdir(base_data_dir):
                        project_path = os.path.join(base_data_dir, project_name)
                        if not os.path.isdir(project_path) or project_name.startswith('_'):
                            continue
                        
                        for lang in os.listdir(project_path):
                            lang_path = os.path.join(project_path, lang)
                            
                            if os.path.isdir(lang_path) and not lang.startswith('_'):
                                project_id = f"{project_name}-{lang}"
                                
                                # 아직 등록되지 않은 프로젝트인 경우
                                if project_id not in project_instances:
                                    friendly_name = f"{project_name} ({lang.upper()})"
                                    print(f"\n🆕 새로운 Cookie 프로젝트 발견: {project_id}")
                                    
                                    # DataProcessor 생성
                                    dp = DataProcessor(lang_path)
                                    dp.project_display_title = friendly_name 
                                    dp.project_name = f"{project_name}"
                                    dp.lang = f"{lang}"
                                    
                                    project_instances[project_id] = dp
                                    
                                    # 백그라운드 스레드 시작
                                    start_data_loader_thread(project_id)
                                    print(f"🚀 Registered: {project_id} as '{friendly_name}' (데이터 로드 중...)")
                                    
                                    # 캐시 무효화
                                    PROJECT_CACHE["list"] = []
                                    PROJECT_CACHE["grouped"] = {}
                
                # Wallchain 프로젝트 스캔
                if os.path.exists(base_wallchain_dir):
                    for project_name in os.listdir(base_wallchain_dir):
                        project_path = os.path.join(base_wallchain_dir, project_name)
                        if not os.path.isdir(project_path) or project_name.startswith('_') or project_name.startswith('.'):
                            continue
                        
                        global_path = os.path.join(project_path, 'global')
                        if os.path.isdir(global_path):
                            project_id = f"wallchain-{project_name}"
                            
                            # 아직 등록되지 않은 프로젝트인 경우
                            if project_id not in wallchain_instances:
                                friendly_name = f"Wallchain: {project_name.upper()}"
                                print(f"\n🆕 새로운 Wallchain 프로젝트 발견: {project_id}")
                                
                                # DataProcessorWallchain 생성
                                dp = DataProcessorWallchain(global_path)
                                dp.project_display_title = friendly_name 
                                dp.project_name = f"{project_name}"
                                
                                wallchain_instances[project_id] = dp
                                
                                # 백그라운드 스레드 시작
                                start_wallchain_loader_thread(project_id)
                                print(f"🌊 Registered: {project_id} as '{friendly_name}' (데이터 로드 중...)")
                                
                                # 캐시 무효화
                                WALLCHAIN_CACHE["list"] = []
                                WALLCHAIN_CACHE["grouped"] = {}
                
            except Exception as e:
                print(f"[프로젝트 스캐너] 오류: {e}")
    
    thread = threading.Thread(target=periodic_scanner, daemon=True)
    thread.start()
    print("[프로젝트 스캐너] 5분마다 새 프로젝트 탐색 시작")

# ===================== KAITO FUNCTIONS =====================

def get_cached_kaito_projects():
    """Kaito 프로젝트 목록 캐시 (5분마다 자동 갱신)"""
    current_time = time.time()
    
    if not kaito_processor:
        return []
    
    if KAITO_CACHE["list"] and (current_time - KAITO_CACHE["last_updated"] < CACHE_INTERVAL):
        return KAITO_CACHE["list"]
    
    # 캐시 갱신
    projects = kaito_processor.scan_projects()
    KAITO_CACHE["list"] = projects
    KAITO_CACHE["last_updated"] = current_time
    print(f"[Kaito 캐시 갱신] {len(projects)}개 프로젝트 - {datetime.now().strftime('%H:%M:%S')}")
    return projects

def init_kaito_on_startup():
    """Kaito 프로세서 초기화"""
    global kaito_processor
    
    print("\n🎯 [Kaito 초기화] 통합 DB 프로세서 생성...")
    kaito_processor = DataProcessorKaito()
    print("✅ [Kaito] 통합 DB 생성 완료")

def start_kaito_data_loader():
    """Kaito 데이터 로더 스레드 (단일 스레드로 순차 처리)"""
    def kaito_periodic_loader():
        print("[Kaito] 데이터 로더 스레드 시작")
        
        # 최초 한 번 전체 로드
        try:
            print("[Kaito] 초기 데이터 로드 시작...")
            projects = kaito_processor.scan_projects()
            
            for project in projects:
                timeframes = ['7D', '30D', '90D', '180D', '360D']
                
                for timeframe in timeframes:
                    new_files = kaito_processor.check_new_files(project, timeframe)
                    
                    if new_files:
                        print(f"[Kaito] {project}/{timeframe}: {len(new_files)}개 파일 로드 중...")
                        
                        for filepath in new_files:
                            data = kaito_processor.load_json_file(filepath)
                            
                            if data:
                                # 타임스탬프 추출
                                filename = os.path.basename(filepath)
                                timestamp_str = filename.replace('.json', '').replace('_', '-')
                                
                                # 데이터 삽입
                                kaito_processor.insert_data(project, timeframe, timestamp_str, data)
                        
                        print(f"[Kaito] {project}/{timeframe}: 완료 ✓")
            
            print("[Kaito] ✅ 초기 데이터 로드 완료")
        except Exception as e:
            print(f"[Kaito] ❌ 초기 로드 오류: {e}")
        
        # 주기적으로 신규 파일 체크 (30초마다)
        while True:
            try:
                time.sleep(30)
                
                projects = kaito_processor.scan_projects()
                new_data_found = False
                
                for project in projects:
                    timeframes = ['7D', '30D', '90D', '180D', '360D']
                    
                    for timeframe in timeframes:
                        new_files = kaito_processor.check_new_files(project, timeframe)
                        
                        if new_files:
                            if not new_data_found:
                                print(f"\n[Kaito] 신규 데이터 발견...")
                                new_data_found = True
                            
                            print(f"[Kaito] {project}/{timeframe}: {len(new_files)}개 파일")
                            
                            for filepath in new_files:
                                data = kaito_processor.load_json_file(filepath)
                                
                                if data:
                                    filename = os.path.basename(filepath)
                                    timestamp_str = filename.replace('.json', '').replace('_', '-')
                                    kaito_processor.insert_data(project, timeframe, timestamp_str, data)
                            
                            print(f"[Kaito] {project}/{timeframe}: 완료 ✓")
                
                if new_data_found:
                    print("[Kaito] ✅ 신규 데이터 로드 완료\n")
                    # 캐시 무효화
                    KAITO_CACHE["list"] = []
                    
            except Exception as e:
                print(f"[Kaito] 데이터 로드 오류: {e}")
    
    thread = threading.Thread(target=kaito_periodic_loader, daemon=True)
    thread.start()

# ===================== END KAITO FUNCTIONS =====================

                
def render_error(error_message, project_name=None):
    try:
        project = project_name or "unknown"
        all_projects = get_cached_projects()
        all_wallchain_projects = get_cached_wallchain_projects()
        grouped_projects = get_grouped_projects()
        grouped_wallchain = get_grouped_wallchain_projects()
        lang = get_language()  # 현재 설정된 언어 가져오기
        return template('error.html',
                       current_project=project,
                       project=project,
                       current_page="",
                       lang=lang,
                       all_projects=all_projects,
                       all_wallchain_projects=all_wallchain_projects,
                       grouped_projects=grouped_projects,
                       grouped_wallchain=grouped_wallchain,
                       kaito_projects=get_cached_kaito_projects(),
                       error_message=error_message,
                       project_instances=project_instances,
                       json=json)
    except ValueError as e:
        return render_error(str(e), projectname)  # 통일된 에러 렌더링
                
# 프로젝트 하위 경로 처리
@app.route('/<projectname>/static/<filepath:path>')
def serve_project_static(projectname, filepath):
    res = static_file(filepath, root='./static')
    # 정적 파일 캐시 헤더 (1년) - 브라우저 캐싱
    response.set_header('Cache-Control', 'public, max-age=31536000, immutable')
    return res

@app.route('/static/<filename:path>')
def send_static(filename):
    res = static_file(filename, root='./static')
    # 정적 파일 캐시 헤더 (1년) - 브라우저 캐싱
    response.set_header('Cache-Control', 'public, max-age=31536000, immutable')
    return res

@app.route('/robots.txt')
def robots():
    return static_file('robots.txt', root='./static')

@app.route('/favicon.ico')
def favicon():
    res = static_file('favicon.ico', root='./static')
    response.set_header('Cache-Control', 'public, max-age=86400')  # 1일
    return res

# ===================== GLOBAL DATA MANAGEMENT =====================

def update_global_rankings():
    """글로벌 DB 갱신 - 모든 프로젝트의 최신 순위 정보 수집"""
    print(f"\n{'='*60}")
    print(f"[글로벌 DB 갱신 시작] {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}")
    
    try:
        # 배치 업데이트 시작 (임시 테이블 생성)
        global_manager.begin_batch_update()
        
        # 메모리에 데이터 수집
        users_batch = {}  # {infoName: (infoName, displayName, imageUrl, wal_score)}
        rankings_batch = []  # [(infoName, projectName, timeframe, ...)]
        
        # Cookie 프로젝트 데이터 수집
        for project_name, dp in list(project_instances.items()):
            try:
                print(f"[Cookie] {project_name} 처리 중...")
                
                for timeframe in dp.timeframes:
                    # 최신 타임스탬프의 데이터 가져오기
                    with sqlite3.connect(dp.db_path) as conn:
                        cursor = conn.cursor()
                        cursor.execute(
                            "SELECT MAX(timestamp) FROM snaps WHERE timeframe = ?",
                            (timeframe,)
                        )
                        latest_ts = cursor.fetchone()[0]
                        
                        if not latest_ts:
                            continue
                        
                        # 해당 타임스탬프의 모든 유저 데이터
                        cursor.execute('''
                            SELECT username, displayName, profileImageUrl, 
                                   rank, cSnapsPercentRank, snapsPercent, cSnapsPercent,
                                   followers, smartFollowers
                            FROM snaps 
                            WHERE timestamp = ? AND timeframe = ?
                        ''', (latest_ts, timeframe))
                        
                        rows = cursor.fetchall()
                        
                        for row in rows:
                            username = row[0]
                            display_name = row[1]
                            image_url = row[2]
                            ms_rank = row[3]  # rank -> ms_rank
                            cms_rank = row[4]  # cSnapsPercentRank -> cms_rank
                            ms_percent = row[5]  # snapsPercent -> ms_percent
                            cms_percent = row[6]  # cSnapsPercent -> cms_percent
                            followers = row[7] if len(row) > 7 else None
                            smart_followers = row[8] if len(row) > 8 else None
                            
                            # 유저 정보 수집 (wallchain 우선이므로 없을 때만)
                            if username not in users_batch:
                                users_batch[username] = (username, display_name, image_url, None,
                                                        smart_followers, None, followers)
                            
                            # 순위 정보 수집
                            rankings_batch.append((
                                username, project_name, timeframe, 
                                ms_rank, cms_rank, ms_percent, cms_percent, None
                            ))
                
                print(f"[Cookie] {project_name} 완료 ✓")
                
            except Exception as e:
                print(f"[Cookie] {project_name} 오류: {e}")
        
        # Wallchain 프로젝트 데이터 수집
        for project_name, dp in list(wallchain_instances.items()):
            try:
                print(f"[Wallchain] {project_name} 처리 중...")
                
                for timeframe in dp.timeframes:
                    # 최신 타임스탬프의 데이터 가져오기
                    with sqlite3.connect(dp.db_path) as conn:
                        cursor = conn.cursor()
                        cursor.execute(
                            "SELECT MAX(timestamp) FROM leaderboard WHERE timeframe = ?",
                            (timeframe,)
                        )
                        latest_ts = cursor.fetchone()[0]
                        
                        if not latest_ts:
                            continue
                        
                        # 해당 타임스탬프의 모든 유저 데이터
                        cursor.execute('''
                            SELECT username, name, imageUrl, score, 
                                   position, positionChange, mindsharePercentage
                            FROM leaderboard 
                            WHERE timestamp = ? AND timeframe = ?
                        ''', (latest_ts, timeframe))
                        
                        rows = cursor.fetchall()
                        
                        for row in rows:
                            username = row[0]  # wallchain의 username (실제 X 핸들, infoName으로 사용)
                            display_name = row[1]  # wallchain의 name (표시 이름)
                            image_url = row[2]
                            score = row[3]
                            position = row[4]
                            position_change = row[5]
                            mindshare_percentage = row[6]
                            
                            # 유저 정보 수집 (wallchain이 최우선이지만 팔로워 정보는 유지)
                            if username in users_batch:
                                # 이미 있으면 wallchain 정보만 업데이트 (팔로워 정보는 유지)
                                existing = users_batch[username]
                                users_batch[username] = (username, display_name, image_url, score,
                                                        existing[4], existing[5], existing[6])  # 팔로워 정보 유지
                            else:
                                # 없으면 새로 추가 (팔로워 정보 없음)
                                users_batch[username] = (username, display_name, image_url, score,
                                                        None, None, None)
                            
                            # 순위 정보 수집
                            rankings_batch.append((
                                username, project_name, timeframe,
                                position, None, mindshare_percentage, None, position_change
                            ))
                
                print(f"[Wallchain] {project_name} 완료 ✓")
                
            except Exception as e:
                print(f"[Wallchain] {project_name} 오류: {e}")
        
        # Kaito 프로젝트 데이터 수집
        if kaito_processor:
            try:
                print(f"[Kaito] 데이터 수집 중...")
                
                # Kaito DB에서 최신 데이터 가져오기
                with sqlite3.connect('./data/kaito/kaito_projects.db') as conn:
                    cursor = conn.cursor()
                    
                    # 각 프로젝트별 최신 타임스탬프 찾기
                    cursor.execute('''
                        SELECT projectName, timeframe, MAX(timestamp) as latest_ts
                        FROM rankings
                        GROUP BY projectName, timeframe
                    ''')
                    
                    latest_timestamps = cursor.fetchall()
                    
                    # 고유 프로젝트 목록 추출 (중복 제거)
                    unique_projects = set()
                    for project_name, timeframe, latest_ts in latest_timestamps:
                        unique_projects.add(project_name)
                    
                    print(f"[Kaito] 발견된 프로젝트 수: {len(unique_projects)}")
                    
                    for project_name, timeframe, latest_ts in latest_timestamps:
                        # 해당 프로젝트/timeframe의 최신 데이터 가져오기
                        cursor.execute('''
                            SELECT handle, displayName, imageId, rank, mindshare, smartFollower, follower
                            FROM rankings
                            WHERE projectName = ? AND timeframe = ? AND timestamp = ?
                        ''', (project_name, timeframe, latest_ts))
                        
                        rows = cursor.fetchall()
                        
                        for row in rows:
                            handle = row[0]  # handle (@ 없이 저장됨)
                            display_name = row[1]
                            image_id = row[2]
                            rank = row[3]
                            mindshare_str = row[4]  # "22.66%" 형식
                            smart_follower_str = row[5] if len(row) > 5 else None
                            follower_str = row[6] if len(row) > 6 else None
                            
                            # mindshare를 숫자로 변환
                            try:
                                mindshare_value = float(mindshare_str.rstrip('%'))
                            except:
                                mindshare_value = 0.0
                            
                            # 팔로워 수를 정수로 변환
                            try:
                                smart_follower = int(smart_follower_str.replace(',', '')) if smart_follower_str else None
                            except:
                                smart_follower = None
                            
                            try:
                                follower = int(follower_str.replace(',', '')) if follower_str else None
                            except:
                                follower = None
                            
                            # 이미지 URL 생성
                            if image_id:
                                image_url = image_id  # 숫자 ID만 저장 (wallchain/cookie 우선순위 로직에서 처리)
                            else:
                                image_url = ""
                            
                            # 유저 정보 수집
                            if handle in users_batch:
                                # 이미 있으면 kaito 정보만 업데이트 (다른 정보는 유지)
                                existing = users_batch[handle]
                                # 이미지는 숫자 ID가 아닌 경우만 유지 (wallchain/cookie 우선)
                                final_image = existing[2] if existing[2] and not existing[2].isdigit() else image_url
                                users_batch[handle] = (handle, existing[1], final_image, existing[3],
                                                      existing[4], smart_follower, follower)  # kaito_smart와 follower 업데이트
                            else:
                                # 없으면 새로 추가
                                users_batch[handle] = (handle, display_name, image_url, None,
                                                      None, smart_follower, follower)
                            
                            # 순위 정보 수집 (kaito- prefix 추가)
                            full_project_name = f"kaito-{project_name}"
                            rankings_batch.append((
                                handle, full_project_name, timeframe,
                                rank, None, mindshare_value, None, None
                            ))
                
                print(f"[Kaito] 데이터 수집 완료 ✓")
                
            except Exception as e:
                print(f"[Kaito] 데이터 수집 오류: {e}")
                import traceback
                traceback.print_exc()
        
        # 배치 삽입
        print(f"[글로벌 DB] 배치 삽입 중... (유저: {len(users_batch)}, 순위: {len(rankings_batch)})")
        global_manager.batch_insert_users(list(users_batch.values()))
        global_manager.batch_insert_rankings(rankings_batch)
        
        # 원자적 교체
        global_manager.commit_batch_update()
        
        # 갱신되지 않은 row의 ms, cms를 0으로 설정 (OUT OF RANK 처리)
        print("[글로벌 DB] OUT OF RANK 유저 처리 중...")
        try:
            # 이번에 수집된 (infoName, projectName, timeframe) 조합
            collected_keys = set()
            for batch in rankings_batch:
                infoName, projectName, timeframe = batch[0], batch[1], batch[2]
                collected_keys.add((infoName, projectName, timeframe))
            
            # DB에서 갱신되지 않은 row 찾아서 ms, cms를 0으로
            with sqlite3.connect('./data/global_rankings.db') as conn:
                cursor = conn.cursor()
                
                # 모든 rankings의 key 가져오기
                cursor.execute('SELECT infoName, projectName, timeframe FROM rankings')
                all_rows = cursor.fetchall()
                
                out_of_rank_count = 0
                for row in all_rows:
                    key = (row[0], row[1], row[2])
                    if key not in collected_keys:
                        # 이번에 수집되지 않은 row -> ms, cms를 0으로
                        cursor.execute('''
                            UPDATE rankings 
                            SET ms = 0, cms = 0 
                            WHERE infoName = ? AND projectName = ? AND timeframe = ?
                        ''', (row[0], row[1], row[2]))
                        out_of_rank_count += 1
                
                conn.commit()
                print(f"[글로벌 DB] OUT OF RANK 처리 완료: {out_of_rank_count}건")
        except Exception as e:
            print(f"[글로벌 DB] OUT OF RANK 처리 오류: {e}")
        
        print(f"\n{'='*60}")
        print(f"[글로벌 DB 갱신 완료] {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*60}\n")
        
    except Exception as e:
        print(f"[글로벌 DB 갱신 실패] {e}")
        import traceback
        traceback.print_exc()

def schedule_global_updates():
    """매 시간 15분에 글로벌 DB 갱신 스케줄링"""
    
    def scheduled_update():
        """스케줄된 갱신 작업 (로그 추가)"""
        print(f"\n[글로벌 DB 스케줄러] 정기 갱신 트리거됨 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        update_global_rankings()
    
    schedule.every().hour.at(":15").do(scheduled_update)
    
    # DB가 비어있으면 즉시 갱신, 아니면 5분 후 갱신
    def initial_update():
        try:
            # 프로젝트 초기화가 완료될 때까지 대기 (최대 60초)
            print("[글로벌 DB] 프로젝트 초기화 대기 중...")
            wait_time = 0
            while (not project_instances or not wallchain_instances) and wait_time < 60:
                time.sleep(1)
                wait_time += 1
            
            if not project_instances and not wallchain_instances and not kaito_processor:
                print("[글로벌 DB] 경고: 프로젝트가 초기화되지 않았습니다.")
                return
            
            kaito_count = len(get_cached_kaito_projects()) if kaito_processor else 0
            print(f"[글로벌 DB] 프로젝트 초기화 완료 - Cookie: {len(project_instances)}, Wallchain: {len(wallchain_instances)}, Kaito: {kaito_count}")
            
            # 데이터베이스에 데이터가 있는지 확인
            conn = sqlite3.connect('./data/global_rankings.db')
            cursor = conn.cursor()
            cursor.execute('SELECT COUNT(*) FROM users')
            count = cursor.fetchone()[0]
            conn.close()
            
            if count == 0:
                print("[글로벌 DB] 데이터가 없음 - 즉시 갱신 시작")
                update_global_rankings()
            else:
                print(f"[글로벌 DB] 기존 데이터 {count}개 확인 - 5분 후 갱신 예정")
                time.sleep(300)  # 5분 대기
                update_global_rankings()
        except Exception as e:
            print(f"[글로벌 DB 초기화 오류] {e}")
            # 오류 발생 시에도 프로젝트가 있으면 갱신 시도
            if project_instances or wallchain_instances:
                print("[글로벌 DB] 오류 발생했지만 갱신 시도...")
                update_global_rankings()
    
    threading.Thread(target=initial_update, daemon=True).start()
    
    # 스케줄러 실행
    def run_scheduler():
        print(f"[글로벌 DB 스케줄러] 백그라운드 실행 시작 - 매 시간 15분에 갱신")
        while True:
            schedule.run_pending()
            time.sleep(30)
    
    threading.Thread(target=run_scheduler, daemon=True).start()
    print("[글로벌 DB 스케줄러] 설정 완료 ✓")

@app.route('/ref')
@app.route('/')
def home_redirect():
    """
    루트 경로 접근 시 DEFAULT_PROJECT로 강제 리디렉션
    """
    # 1. 어떤 경로로 들어왔는지 확인
    path = request.path
    
    if path == '/ref':
        # print("[로그] 리퍼럴 경로(/ref)를 통해 접속함")
        log_access('home_redirect', "ref")
        # 리퍼럴 전용 처리가 필요하다면 여기서 수행
    else:
        # print("[로그] 기본 경로(/)를 통해 접속함")
        log_access('home_redirect', "UNKNOWN")
    # HTTP 상태 코드 302 (Found) 또는 301 (Moved Permanently)와 함께 리디렉션
    return redirect(f'/spaace-ko/leaderboard', code=302)
@app.route('/set_lang/<lang>')
def set_language(lang):
    """
    언어 설정을 쿠키에 저장하고 이전 페이지로 리디렉션
    """
    if lang not in ['ko', 'en']:
        lang = 'ko'
    
    # 쿠키 저장 (유효기간 30일)
    response.set_cookie('lang', lang, path='/', max_age=30*24*60*60)
    
    # 이전 페이지(Referer)로 돌아가기, 없으면 홈으로
    redirect_url = request.environ.get('HTTP_REFERER', '/')
    return redirect(redirect_url)
def get_flag(region='en'):
    if region == 'en':
        return "🌐"
    elif region == 'ko':
        return "🇰🇷"
    elif region == 'zh':
        return "🇨🇳"
    elif region == 'pt':
        return "🇵🇹"
    elif region == 'es':
        return "🇪🇸"
    return "🌐"

def get_language():
    """
    쿠키에서 언어 설정을 가져옴 (기본값 'ko')
    """
    return request.get_cookie('lang', 'ko')

# ===================== GLOBAL SEARCH ROUTES =====================

@app.route('/user-lookup')
def user_lookup_page():
    """글로벌 검색 페이지"""
    log_access('user_lookup', 'global_search')
    lang = get_language()
    
    all_projects = get_cached_projects()
    all_wallchain_projects = get_cached_wallchain_projects()
    grouped_projects = get_grouped_projects()
    grouped_wallchain = get_grouped_wallchain_projects()
    
    # 현재 페이지를 'SEARCH'로 설정하여 네비게이션에서 표시
    return template('user_lookup.html',
                   lang=lang,
                   current_page='user_lookup',
                   project='SEARCH',
                   all_projects=all_projects,
                   all_wallchain_projects=all_wallchain_projects,
                   grouped_projects=grouped_projects,
                   grouped_wallchain=grouped_wallchain,
                   kaito_projects=get_cached_kaito_projects(),
                   t={})

@app.route('/api/user-search')
def api_user_search():
    """유저 검색 자동완성 API"""
    response.content_type = 'application/json; charset=utf-8'
    
    # URL 쿼리 파라미터에서 직접 가져오기 (UTF-8 디코딩 보장)
    import urllib.parse
    query_string = request.environ.get('QUERY_STRING', '')
    if query_string:
        parsed = urllib.parse.parse_qs(query_string)
        query = parsed.get('q', [''])[0].strip()
    else:
        query = ''
    
    if len(query) < 1:
        return json.dumps([], ensure_ascii=False)
    
    try:
        results = global_manager.search_users(query, limit=10)
        return json.dumps(results, ensure_ascii=False)
    except Exception as e:
        print(f"[API Error] user-search: {e}")
        return json.dumps([], ensure_ascii=False)

@app.route('/api/user-data/<username>')
def api_user_data(username):
    """특정 유저의 전체 데이터 API"""
    response.content_type = 'application/json; charset=utf-8'
    
    # 검색 로그 기록
    log_access('user_lookup',  username)
    
    try:
        data = global_manager.get_user_data(username)
        
        if not data:
            return json.dumps({'error': 'User not found'}, ensure_ascii=False)
        
        return json.dumps(data, ensure_ascii=False)
    except Exception as e:
        print(f"[API Error] user-data: {e}")
        return json.dumps({'error': str(e)}, ensure_ascii=False)

# ===================== END GLOBAL ROUTES =====================

@app.route('/leaderboard')
@app.route('/leaderboard/')
@app.route('/compare')
@app.route('/compare/')
def home_redirect():
    """
    루트 경로 접근 시 DEFAULT_PROJECT로 강제 리디렉션
    """
    log_access('home_redirect', "UNKNOWN")
    # HTTP 상태 코드 302 (Found) 또는 301 (Moved Permanently)와 함께 리디렉션
    return redirect(f'/spaace-en/leaderboard', code=302)
@app.route('/<projectname>/user/')
@app.route('/<projectname>/user')
def home_redirect(projectname):
    """
    루트 경로 접근 시 DEFAULT_PROJECT로 강제 리디렉션
    """
    log_access('home_redirect', projectname)
    # HTTP 상태 코드 302 (Found) 또는 301 (Moved Permanently)와 함께 리디렉션
    return redirect(f'/'+projectname, code=302)

# 동적 프로젝트 라우팅
@app.route('/<projectname>')
@app.route('/<projectname>/')
@app.route('/cookie/<projectname>')
@app.route('/cookie/<projectname>/')
def project_index(projectname):
    log_access('user_search', projectname)
    # 🚨 [필수 추가] /favicon.ico 요청이 실수로 앱에 도달했을 때 404 반환
    lang = get_language()  # 현재 설정된 언어 가져오기
    if projectname.lower() == 'favicon.ico':
        # bottle.abort(404)를 사용하여 명시적으로 404 Not Found를 반환합니다.
        abort(404)
    if projectname not in project_instances:
        # favicon.ico나 wp-admin 같은 경로 처리
        log_access('invalid_access', projectname)
        return redirect(f'/spaace-en/leaderboard', code=302)
        # return render_error("존재하지 않는 프로젝트", projectname)
    try:
        dp = get_data_processor(projectname)
        timeframe = request.query.get('timeframe', 'TOTAL')
        display_project_name = dp.project_name
        # {'ko': '🇰🇷', 'en': '🌐', 'zh': '🇨🇳'}

        display_project_name = get_flag(dp.lang) +" " + display_project_name
        # 모든 사용자 목록 - 7D, 14D, 30D, TOTAL에서 중복 제거하여 가져옴
        all_users = dp.get_all_usernames_from_multiple_timeframes(['7D', '14D', '30D', 'TOTAL'])
        all_projects = get_cached_projects()
        all_wallchain_projects = get_cached_wallchain_projects()
        grouped_projects = get_grouped_projects()
        grouped_wallchain = get_grouped_wallchain_projects()
        return template('index.html', 
                       current_project=projectname,
                       display_project_name=display_project_name,
                       lang=lang,
                       current_page="",
                       project=projectname,
                       all_projects=all_projects,
                       all_wallchain_projects=all_wallchain_projects,
                       grouped_projects=grouped_projects,
                       grouped_wallchain=grouped_wallchain,
                       kaito_projects=get_cached_kaito_projects(),
                       all_users=all_users,
                       timeframe=timeframe,
                       timeframes=dp.timeframes)
    except ValueError as e:
        return render_error(str(e), projectname)

@app.route('/<projectname>/leaderboard')
@app.route('/cookie/<projectname>/leaderboard')
def project_leaderboard(projectname):
    log_access('project_leaderboard', projectname)
    lang = get_language()  # 현재 설정된 언어 가져오기
    if projectname not in project_instances:
        # favicon.ico나 wp-admin 같은 경로 처리
        log_access('invalid_access', projectname)
        return redirect(f'/spaace-en/leaderboard', code=302)
        # return render_error("존재하지 않는 프로젝트", projectname)
    try:
        dp = get_data_processor(projectname)

        timeframe = request.query.get('timeframe', 'TOTAL')
        timestamp1 = request.query.get('timestamp1', '')
        timestamp2 = request.query.get('timestamp2', '')
        
        # ⭐ [수정] metric 파라미터 추가 및 기본값 'snapsPercent' 설정 ⭐
        metric = request.query.get('metric', 'snapsPercent') 
        _col_metric = ""
        # ⭐⭐⭐ 1. 컬럼 변수 정의를 여기로 옮깁니다. ⭐⭐⭐
        if metric == 'cSnapsPercent':
            if lang =='ko':
                metric_display_name = "c마쉐"
            else:
                metric_display_name = "cMS"
            mindshare_change_col = 'c_mindshare_change' 
            prev_mindshare_col = 'prev_c_mindshare'
            curr_mindshare_col = 'curr_c_mindshare'
            _col_metric="c"
        else:
            # 기본값 'snapsPercent'
            if lang =='ko':
                metric_display_name = "마쉐"
            else:
                metric_display_name = "MS"
            mindshare_change_col = 'mindshare_change'
            prev_mindshare_col = 'prev_mindshare'
            curr_mindshare_col = 'curr_mindshare'
        # ⭐⭐⭐ 컬럼 변수 정의 끝 ⭐⭐⭐
        # 사용 가능한 타임스탬프 목록
        timestamps = dp.get_available_timestamps(timeframe)
        
        # 1. 사용 가능한 타임스탬프 개수 확인
        num_ts = len(timestamps)

        if num_ts > 0:
            if not timestamp1 or timestamp1 not in timestamps:
                # 2. -9 인덱스를 시도하되, 데이터가 부족하면 0번(최초 데이터)을 선택
                # max(0, num_ts - 9)를 사용하면 데이터가 5개뿐일 때 -4가 아닌 0번 인덱스를 잡습니다.
                try:
                    # 원래 의도하신 -9 인덱스 시도
                    timestamp1 = timestamps[-10]
                except IndexError:
                    # -9가 없을 경우, 리스트의 가장 첫 번째([0]) 데이터를 선택 (최대 가용 범위)
                    timestamp1 = timestamps[0]
                    
            if not timestamp2 or timestamp2 not in timestamps:
                # timestamp2는 리스트의 가장 마지막(최신) 값으로 설정
                timestamp2 = timestamps[-1]
        else:
            timestamp1 = timestamp2 = ''
        if not timestamp2 or timestamp2 not in timestamps:
            timestamp2 = timestamps[-1] if timestamps else ''
        
        # 리더보드 분석 결과
        compare_data = pd.DataFrame()
        
        if timestamp1 and timestamp2:
            # ⭐ 수정: metric 파라미터 전달 ⭐
            compare_data = dp.compare_leaderboards(timestamp1, timestamp2, timeframe, metric)
        # 데이터 테이블을 HTML로 변환
        if not compare_data.empty:
            # 변화량에 화살표 추가하고 스타일 적용
            compare_data['rank_change_display'] = compare_data['rank_change'].apply(
                lambda x: f"{x}" if x > 0 else (f"{x}" )
            )
            compare_data['mindshare_change_display'] = compare_data[mindshare_change_col].apply( 
                lambda x: f"{x:.4f}" if x > 0 else (f"{x:.4f}" )
            )
            
            if lang == 'ko':
                # HTML 테이블 생성
                table_html = f"""
                <table id="leaderboardTable" class="table table-striped table-hover">
                    <thead>
                        <tr>
                            <th>사용자</th>
                            <th>이전 순위</th>
                            <th>현재 순위</th>
                            <th>순위 변화</th>
                            <th>이전 {_col_metric}마쉐</th>
                            <th>현재 {_col_metric}마쉐</th>
                            <th>{_col_metric}마쉐 변화</th>
                        </tr>
                    </thead>
                    <tbody>
                """
            else:
                                # HTML 테이블 생성
                table_html = f"""
                <table id="leaderboardTable" class="table table-striped table-hover">
                    <thead>
                        <tr>
                            <th>User</th>
                            <th>Pre Rank</th>
                            <th>Cur Rank</th>
                            <th>Rank Change</th>
                            <th>Pre {_col_metric}MS</th>
                            <th>Cur {_col_metric}MS</th>
                            <th>{_col_metric}MS Change</th>
                        </tr>
                    </thead>
                    <tbody>
                """
            for i, row in enumerate(compare_data.itertuples(), 1):
                prev_rank = row.prev_rank
                curr_rank = row.curr_rank
                prev_mindshare_value = getattr(row, prev_mindshare_col)
                curr_mindshare_value = getattr(row, curr_mindshare_col)
                mindshare_change_value = getattr(row, mindshare_change_col)
                
                # 순위 변화 및 마쉐 변화 HTML 생성
                if prev_rank == 9999 and curr_rank != 9999:
                    rank_change_html = '<span class="badge bg-success" data-order="0">NEW</span>'
                    mindshare_change_html = '<span class="badge bg-success" data-order="0">NEW</span>'
                elif prev_rank != 9999 and curr_rank == 9999:
                    rank_change_html = '<span class="badge bg-secondary" data-order="0">OUT</span>'
                    mindshare_change_html = '<span class="badge bg-secondary" data-order="0">OUT</span>'
                elif prev_rank != 9999 and curr_rank != 9999:
                    change = prev_rank - curr_rank
                    if change > 0:
                        rank_change_html = f'<span class="text-success" data-order="{change}">↑ {change}</span>'
                    elif change < 0:
                        rank_change_html = f'<span class="text-danger" data-order="{change}">↓ {abs(change)}</span>'
                    else:
                        rank_change_html = '<span class="text-muted" data-order="0">-</span>'
                    
                    # 마쉐 변화
                    if mindshare_change_value > 0:
                        mindshare_change_html = f'<span class="text-success" data-order="{mindshare_change_value:.4f}">+{mindshare_change_value:.4f}</span>'
                    elif mindshare_change_value < 0:
                        mindshare_change_html = f'<span class="text-danger" data-order="{mindshare_change_value:.4f}">{mindshare_change_value:.4f}</span>'
                    else:
                        mindshare_change_html = '<span class="text-muted" data-order="0">-</span>'
                else:
                    rank_change_html = '<span class="text-muted" data-order="0">-</span>'
                    mindshare_change_html = '<span class="text-muted" data-order="0">-</span>'
                
                table_html += f"""
                    <tr>
                        <td>
                            <div class="d-flex align-items-center">
                                <img src="{row.profileImageUrl}" alt="{row.displayName}" class="me-2" style="width:32px;height:32px;border-radius:50%;">
                                <div>
                                    <strong>{row.displayName}</strong><br>
                                    <small class="text-muted">@{row.username}</small><a href="/cookie/{projectname}/user/{row.username}" class="user-link" title="유저 분석">🔍</a>
                                </div>
                            </div>
                        </td>
                        <td>{int(prev_rank) if prev_rank != 9999 else '-'}</td>
                        <td>{int(curr_rank) if curr_rank != 9999 else '-'}</td>
                        <td>{rank_change_html}</td>
                        <td>{prev_mindshare_value:.4f}</td>
                        <td>{curr_mindshare_value:.4f}</td>
                        <td>{mindshare_change_html}</td>
                    </tr>
                    """
            
            table_html += """
                </tbody>
            </table>
            """
            
        else:
            table_html = "<p>비교할 데이터가 없습니다.</p>"
        
        # 타임스탬프 포맷팅 (가독성 향상)
        formatted_timestamps = {}
        for ts in timestamps:
            try:
                dt = datetime.strptime(ts, '%Y-%m-%d %H:%M:%S')
                formatted_timestamps[ts] = dt.strftime('%Y-%m-%d %H:%M')
            except:
                formatted_timestamps[ts] = ts
        
        # Display용 timestamp 계산
        timestamp1_display = formatted_timestamps.get(timestamp1, timestamp1)
        timestamp2_display = formatted_timestamps.get(timestamp2, timestamp2)
        
        all_projects = get_cached_projects()
        all_wallchain_projects = get_cached_wallchain_projects()
        grouped_projects = get_grouped_projects()
        grouped_wallchain = get_grouped_wallchain_projects()
        display_project_name = dp.project_name
        # {'ko': '🇰🇷', 'en': '🌐', 'zh': '🇨🇳'}

        display_project_name = get_flag(dp.lang) +" " + display_project_name
        
        return template('leaderboard.html', 
                       project=projectname,
                       lang=lang,
                       display_project_name=display_project_name,
                       current_project=projectname,
                       current_page="leaderboard",
                       all_projects=all_projects,
                       all_wallchain_projects=all_wallchain_projects,
                       grouped_projects=grouped_projects,
                       grouped_wallchain=grouped_wallchain,
                       kaito_projects=get_cached_kaito_projects(),
                       timeframe=timeframe,
                       timeframes=dp.timeframes,
                       timestamps=json.dumps(timestamps),
                       metric=metric, # 👈 이 줄을 추가해야 합니다.
                       metric_display_name=metric_display_name,
                       _col_metric=_col_metric,
                       formatted_timestamps=json.dumps(formatted_timestamps),
                       timestamp1=timestamp1,
                       timestamp2=timestamp2,
                       timestamp1_display=timestamp1_display,
                       timestamp2_display=timestamp2_display,
                       table_html=table_html)
    except ValueError as e:
        return render_error(str(e), projectname)


# 사용자 상세 분석 페이지
@app.route('/<projectname>/user/<username>')
@app.route('/cookie/<projectname>/user/<username>')
def project_user_analysis(projectname,username):
    log_access('user', projectname, username)
    lang = get_language()  # 현재 설정된 언어 가져오기

    if projectname not in project_instances:
        # favicon.ico나 wp-admin 같은 경로 처리
        log_access('invalid_access', projectname)
        return redirect(f'/spaace-en/leaderboard', code=302)
        # return render_error("존재하지 않는 프로젝트", projectname)
    try:
        # print(projectname)
        dp = project_instances[projectname]
        # print(dp)
        # user_info = dp.get_user_info(username)
        
        # URL 쿼리 파라미터에서 metric 가져오기
        metric = request.query.get('metric', 'snapsPercent')
        timeframe = 'total'
        user_info_by_timeframe = {}
        for tf in dp.timeframes:
            user_info_by_timeframe[tf] = dp.get_user_info_by_timeframe(username, tf)

        # 현재 선택된 metric에 따라 기본으로 보여줄 timeframe의 user_info를 설정
        # user_info = user_info_by_timeframe.get(timeframe, {})
        # if not user_info:
        #     user_info = dp.get_user_info(username) # Total 정보가 없으면, 최신 사용자 정보 가져옴
        
        # 기본적으로 TOTAL 데이터를 사용하되, 특정 timeframe을 선택하지 않은 경우
        user_info = user_info_by_timeframe['TOTAL']
        if not user_info:
            user_info = dp.get_user_info(username) # Total 정보가 없으면, 최신 사용자 정보 가져옴
        
        if lang=='ko':
            title = f"{user_info.get('displayName', username)}의 기간별 변화 분석"
            rank = f"순위"
        else:
            title = f"{user_info['displayName']}'s changes over time"
            rank = f"Rank"
        # metric에 따라 컬럼 이름 동적 결정
        if metric == 'cSnapsPercent':
            rank_col = 'cSnapsPercentRank'
            mindshare_col = 'cSnapsPercent'
            if lang=='ko':
                mindshare_display_name = 'c마인드쉐어'
                rank_display_name = 'c순위' 
            else:
                mindshare_display_name = 'cMS'
                rank_display_name = 'cRank' 
        else: # 기본값: snapsPercent
            rank_col = 'rank'
            mindshare_col = 'snapsPercent'
            if lang=='ko':
                mindshare_display_name = '마인드쉐어'
                rank_display_name = '순위' 
            else:
                mindshare_display_name = 'MS'
                rank_display_name = 'Rank' 
        user_data = dp.get_user_analysis(username)
        
        # 데이터가 있는 timeframe만 필터링
        available_timeframes = []
        for tf in dp.timeframes:
            df = user_data.get(tf, pd.DataFrame())
            if not df.empty:
                available_timeframes.append(tf)
        
        # 데이터가 있는 경우에만 차트 생성
        if not available_timeframes:
            user_chart = ""
        else:
            # subplot_titles를 available_timeframes 기준으로 동적 생성
            subplot_titles_list = tuple(available_timeframes)
            
            # 동적으로 서브플롯 생성
            fig = make_subplots(
                rows=len(available_timeframes), cols=1, 
                subplot_titles=subplot_titles_list,
                vertical_spacing=0.12,
                specs=[[{"secondary_y": True}] for _ in available_timeframes]
            )
            
            # ⭐⭐⭐ [수정 2] 차트 그리기 루프: 순위/마쉐를 하나의 서브플롯에 추가 ⭐⭐⭐
            # available_timeframes만 사용
            for i, tf in enumerate(available_timeframes):
                row_num = i + 1
                df = user_data[tf]
                
                if not df.empty:
                    # 이전 데이터가 있지만 현재 OUT 상태인 경우 더미 데이터 추가
                    if len(df) > 0:
                        latest_timestamp = df['timestamp'].max()
                        # 현재 시점의 데이터 확인
                        timestamps_in_tf = dp.get_available_timestamps(tf)
                        if timestamps_in_tf and len(timestamps_in_tf) > 0:
                            current_timestamp = pd.Timestamp(max(timestamps_in_tf))
                            # 최신 타임스탬프가 현재보다 오래된 경우 (OUT 상태)
                            if latest_timestamp < current_timestamp:
                                # 더미 데이터 추가 (rank=9999, mindshare=0)
                                dummy_row = pd.DataFrame({
                                    'timestamp': [current_timestamp],
                                    rank_col: [9999],
                                    mindshare_col: [0]
                                })
                                df = pd.concat([df, dummy_row], ignore_index=True).sort_values('timestamp')
                    # 1. 순위 변화 (주 Y축: secondary_y=False)
                    fig.add_trace(
                        go.Scatter(
                            x=df['timestamp'], 
                            y=df[rank_col], 
                            mode='lines+markers',
                            name=rank,
                            line=dict(width=1, color='#FF0000'), # 파란색 계열
                            marker=dict(size=2, symbol='circle'),
                            showlegend=False,
                        ),
                        row=row_num, col=1, secondary_y=False
                    )
                    
                    # 2. 마인드쉐어 변화 (보조 Y축: secondary_y=True)
                    fig.add_trace(
                        go.Scatter(
                            x=df['timestamp'], 
                            y=df[mindshare_col], 
                            mode='lines+markers',
                            name=f'{mindshare_display_name}',
                            line=dict(width=1, color='#1F77B4', dash='dot'), # 주황색 계열, 점선으로 구분
                            marker=dict(size=2, symbol='square'),
                            showlegend=False,
                        ),
                        row=row_num, col=1, secondary_y=True
                    )
                    
                    # Y축 설정
                    # 주 Y축 (순위): 제목 설정 및 순위이므로 Y축 반전
                    fig.update_yaxes(
                        title_text=rank, 
                        autorange="reversed", 
                        row=row_num, col=1, secondary_y=False,
                        gridcolor='lightgray',
                        zeroline=True,
                        fixedrange=True
                    )
                    
                    # 보조 Y축 (마인드쉐어): 제목 설정
                    fig.update_yaxes(
                        title_text=f"{mindshare_display_name} (%)", 
                        row=row_num, col=1, secondary_y=True,
                        gridcolor='rgba(0,0,0,0)', # 보조축의 그리드라인은 투명하게 하여 중복 방지
                        fixedrange=True
                    )
                    # X축 설정
                    fig.update_xaxes(
                        row=row_num, col=1, 
                        fixedrange=True
                    )
            
            # 차트 높이를 timeframe 개수에 따라 동적 조정
            chart_height = 300 * len(available_timeframes)
            
            # ⭐⭐⭐ [수정 3] 레이아웃 및 범례 설정 ⭐⭐⭐
            fig.update_layout(
                height=chart_height, 
                width=None, # 클라이언트 CSS에 너비를 맡김
                title_text= title,
                hovermode="x unified", # 툴팁을 통합하여 가독성 향상
                font=dict(size=12, color='#b8b8b8'),
                # dragmode="hovermode",
                showlegend=False,
                paper_bgcolor='#2d2d2d',
                plot_bgcolor='#2d2d2d'
            )
            
            # 서브플롯 제목 글꼴 크기 조정
            fig.update_annotations(font_size=30)
            fig.update_annotations(
                x=0.0, 
                xanchor='left' 
            )   
            user_chart = pio.to_html(fig, 
                                     full_html=False,
                                     include_plotlyjs='cdn',
                                     config={'responsive': True,
                                     'staticPlot': False,
                                     'displayModeBar': True,
                                     'displaylogo': False,
                                     'modeBarButtonsToRemove': [
                                             'zoom2d',      # 줌 버튼 제거
                                             'pan2d',       # 패닝 버튼 제거
                                             'select2d',    # 선택 버튼 제거 (dragmode='select' 기능 차단)
                                             'lasso2d',     # 올가미 버튼 제거
                                             'zoomIn2d',
                                             'zoomOut2d',
                                             'autoscale',
                                             'resetScale2d'
                                         ]
                                     }
                                    )
        try:
            all_users = dp.get_all_users()
            all_projects = get_cached_projects()
            all_wallchain_projects = get_cached_wallchain_projects()
            grouped_projects = get_grouped_projects()
            grouped_wallchain = get_grouped_wallchain_projects()
            
            display_project_name = dp.project_name
            # {'ko': '🇰🇷', 'en': '🌐', 'zh': '🇨🇳'}
            display_project_name = get_flag(dp.lang) +" " + display_project_name

        except AttributeError:
            # 안전을 위해 DataProcessor에 해당 메서드가 없을 경우 빈 리스트로 처리
            all_users = []
            all_projects = []
            all_wallchain_projects = []
            grouped_projects = {}
            grouped_wallchain = {}
        return template('user.html', 
                       project=projectname,
                       display_project_name=display_project_name,
                       lang=lang,
                       current_project=projectname,
                       current_page="user",
                       all_projects=all_projects,
                       all_wallchain_projects=all_wallchain_projects,
                       grouped_projects=grouped_projects,
                       grouped_wallchain=grouped_wallchain,
                       kaito_projects=get_cached_kaito_projects(),
                       username=username,
                       user_chart=user_chart,
                       user_info=user_info,
                       all_users=json.dumps(all_users), # JSON 문자열로 변환
                       timeframe=timeframe,
                       metric=metric, 
                       timeframes=available_timeframes,
                       user_info_by_timeframe=user_info_by_timeframe,
                       rank_col=rank_col,
                       mindshare_col = mindshare_col,
                       json=json)
    except ValueError as e:
        return render_error(str(e), projectname)

# 사용자 비교 페이지
@app.route('/<projectname>/compare')
def project_compare_users(projectname):
    log_access('project_compare', projectname)
    try:
        dp = get_data_processor(projectname)
        timeframe = request.query.get('timeframe', '7D')
        metric = request.query.get('metric', 'snapsPercent')
        users = request.query.getlist('users')
        
        metrics = {
            'snapsPercent': '마인드쉐어',
            'followers': '팔로워 수',
            'smartFollowers': '스마트 팔로워 수',
            'rank': '순위'
        }
        
        all_users = dp.get_all_usernames(timeframe=timeframe)
        
        if users:
            user_comparison = dp.get_user_comparison(users, timeframe, metric)
            
            fig = go.Figure()
            for username, data in user_comparison.items():
                if not data.empty:
                    fig.add_trace(go.Scatter(
                        x=data['timestamp'],
                        y=data[metric],
                        mode='lines+markers',
                        name=username
                    ))
            
            fig.update_layout(
                title=f'선택한 사용자들의 {metrics.get(metric, metric)} 비교 (기간: {timeframe})',
                xaxis_title='시간',
                yaxis_title=metrics.get(metric, metric),
                height=600
            )
            
            comparison_chart = pio.to_html(fig, full_html=False)
        else:
            comparison_chart = "<p>비교할 사용자를 선택하세요.</p>"
        
        return template('compare.html', 
                       project=projectname,
                       current_project=projectname,
                       current_page="compare",
                       comparison_chart=comparison_chart,
                       all_users=all_users,
                       selected_users=users,
                       timeframe=timeframe,
                       metric=metric,
                       metrics=metrics,
                       timeframes=dp.timeframes,
                       json=json)
    except ValueError as e:
        return render_error(str(e), projectname)

# ===================== WALLCHAIN ROUTES =====================

@app.route('/wallchain/<projectname>')
@app.route('/wallchain/<projectname>/')
def wallchain_index(projectname):
    log_access('user_search_wall', projectname)
    lang = get_language()
    
    full_project_name = f"wallchain-{projectname}"
    if full_project_name not in wallchain_instances:
        log_access('invalid_access', projectname)
        return redirect(f'/spaace-en/leaderboard', code=302)
    
    try:
        dp = wallchain_instances[full_project_name]
        
        # timeframe 요청값 가져오기
        requested_timeframe = request.query.get('timeframe', '')
        
        # 사용 가능한 timeframe 중 실제 데이터가 있는 것을 선택
        timeframe = None
        
        if requested_timeframe and requested_timeframe in dp.timeframes:
            # 요청된 timeframe에 데이터가 있는지 확인
            timestamps_check = dp.get_available_timestamps(requested_timeframe)
            if timestamps_check:
                timeframe = requested_timeframe
        
        # 요청된 timeframe이 없거나 데이터가 없으면 사용 가능한 timeframe 중 선택
        if not timeframe:
            # dp.timeframes에서 데이터가 있는 마지막 timeframe 선택
            for tf in reversed(dp.timeframes):
                timestamps_check = dp.get_available_timestamps(tf)
                if timestamps_check:
                    timeframe = tf
                    break
        
        # 그래도 없으면 마지막 timeframe 사용
        if not timeframe:
            timeframe = dp.timeframes[-1] if dp.timeframes else '7d'
        
        # 모든 timeframe에서 사용자 검색 (중복 제거)
        all_users = dp.get_all_usernames_from_all_timeframes()
        
        # 데이터가 있는 timeframe만 필터링
        available_timeframes = []
        for tf in dp.timeframes:
            timestamps_check = dp.get_available_timestamps(tf)
            if timestamps_check:
                available_timeframes.append(tf)
        
        # timeframe 정렬: 7d, 30d, 나머지는 알파벳 순
        def sort_timeframes(tf):
            tf_lower = tf.lower()
            if tf_lower == '7d':
                return (0, tf)
            elif tf_lower == '30d':
                return (1, tf)
            else:
                return (2, tf)
        
        available_timeframes.sort(key=sort_timeframes)
        
        all_wallchain_projects = get_cached_wallchain_projects()
        all_cookie_projects = get_cached_projects()
        grouped_projects = get_grouped_projects()
        grouped_wallchain = get_grouped_wallchain_projects()
        
        return template('index_wall.html', 
                       current_project=full_project_name,
                       display_project_name=dp.project_display_title,
                       lang=lang,
                       current_page="",
                       project=projectname,
                       is_wallchain=True,
                       all_projects=all_cookie_projects,
                       all_wallchain_projects=all_wallchain_projects,
                       grouped_projects=grouped_projects,
                       grouped_wallchain=grouped_wallchain,
                       kaito_projects=get_cached_kaito_projects(),
                       all_users=all_users,
                       timeframe=timeframe,
                       timeframes=available_timeframes)
    except ValueError as e:
        return render_error(str(e), projectname)

@app.route('/wallchain/<projectname>/leaderboard')
def wallchain_leaderboard(projectname):
    log_access('wallchain_leaderboard', projectname)
    lang = get_language()
    
    full_project_name = f"wallchain-{projectname}"
    if full_project_name not in wallchain_instances:
        log_access('invalid_access', projectname)
        return redirect(f'/spaace-en/leaderboard', code=302)
    
    try:
        dp = wallchain_instances[full_project_name]
        
        # timeframe 요청값 가져오기
        requested_timeframe = request.query.get('timeframe', '')
        
        # 사용 가능한 timeframe 중 실제 데이터가 있는 것을 선택
        timeframe = None
        
        if requested_timeframe and requested_timeframe in dp.timeframes:
            # 요청된 timeframe에 데이터가 있는지 확인
            timestamps_check = dp.get_available_timestamps(requested_timeframe)
            if timestamps_check:
                timeframe = requested_timeframe
        
        # 요청된 timeframe이 없거나 데이터가 없으면 사용 가능한 timeframe 중 선택
        if not timeframe:
            # dp.timeframes에서 데이터가 있는 마지막 timeframe 선택
            for tf in reversed(dp.timeframes):
                timestamps_check = dp.get_available_timestamps(tf)
                if timestamps_check:
                    timeframe = tf
                    break
        
        # 그래도 없으면 마지막 timeframe 사용
        if not timeframe:
            timeframe = dp.timeframes[-1] if dp.timeframes else '7d'
        
        timestamp1 = request.query.get('timestamp1', '')
        timestamp2 = request.query.get('timestamp2', '')
        
        timestamps = dp.get_available_timestamps(timeframe)
        num_ts = len(timestamps)
        
        if num_ts > 0:
            if not timestamp1 or timestamp1 not in timestamps:
                # 2. -9 인덱스를 시도하되, 데이터가 부족하면 0번(최초 데이터)을 선택
                # max(0, num_ts - 9)를 사용하면 데이터가 5개뿐일 때 -4가 아닌 0번 인덱스를 잡습니다.
                try:
                    # 원래 의도하신 -2 인덱스 시도
                    timestamp1 = timestamps[-2]
                except IndexError:
                    # -9가 없을 경우, 리스트의 가장 첫 번째([0]) 데이터를 선택 (최대 가용 범위)
                    timestamp1 = timestamps[0]
                    
            if not timestamp2 or timestamp2 not in timestamps:
                # timestamp2는 리스트의 가장 마지막(최신) 값으로 설정
                timestamp2 = timestamps[-1]
        else:
            timestamp1 = timestamp2 = ''
        if not timestamp2 or timestamp2 not in timestamps:
            timestamp2 = timestamps[-1] if timestamps else ''
            
        compare_data = pd.DataFrame()
        
        if timestamp1 and timestamp2:
            compare_data = dp.compare_leaderboards(timestamp1, timestamp2, timeframe)
        
        if not compare_data.empty:
            compare_data['position_change_display'] = compare_data['position_change'].apply(
                lambda x: f"{x}" if x > 0 else (f"{x}")
            )
            compare_data['mindshare_change_display'] = compare_data['mindshare_change'].apply(
                lambda x: f"{x:.4f}" if x > 0 else (f"{x:.4f}")
            )
            
            if lang == 'ko':
                # HTML 테이블 생성
                table_html = """
                <table id="leaderboardTable" class="table table-striped table-hover">
                    <thead>
                        <tr>
                            <th>사용자</th>
                            <th>이전 순위</th>
                            <th>현재 순위</th>
                            <th>순위 변화</th>
                            <th>이전 마쉐</th>
                            <th>현재 마쉐</th>
                            <th>마쉐 변화</th>
                        </tr>
                    </thead>
                    <tbody>
                """
            else:
                # HTML 테이블 생성
                table_html = """
                <table id="leaderboardTable" class="table table-striped table-hover">
                    <thead>
                        <tr>
                            <th>User</th>
                            <th>Pre Rank</th>
                            <th>Cur Rank</th>
                            <th>Rank Change</th>
                            <th>Pre MS</th>
                            <th>Cur MS</th>
                            <th>MS Change</th>
                        </tr>
                    </thead>
                    <tbody>
                """
            
            for i, row in enumerate(compare_data.itertuples(), 1):
                prev_position = row.prev_position
                curr_position = row.curr_position
                
                # 순위 변화 및 마쉐 변화 HTML 생성
                if prev_position == 9999 and curr_position != 9999:
                    position_change_html = '<span class="badge bg-success" data-order="0">NEW</span>'
                    mindshare_change_html = '<span class="badge bg-success" data-order="0">NEW</span>'
                elif prev_position != 9999 and curr_position == 9999:
                    position_change_html = '<span class="badge bg-secondary" data-order="0">OUT</span>'
                    mindshare_change_html = '<span class="badge bg-secondary" data-order="0">OUT</span>'
                elif prev_position != 9999 and curr_position != 9999:
                    change = prev_position - curr_position
                    if change > 0:
                        position_change_html = f'<span class="text-success" data-order="{change}">↑ {change}</span>'
                    elif change < 0:
                        position_change_html = f'<span class="text-danger" data-order="{change}">↓ {abs(change)}</span>'
                    else:
                        position_change_html = '<span class="text-muted" data-order="0">-</span>'
                    
                    # 마쉐 변화
                    ms_change = row.mindshare_change
                    if ms_change > 0:
                        mindshare_change_html = f'<span class="text-success" data-order="{ms_change:.4f}">+{ms_change:.4f}</span>'
                    elif ms_change < 0:
                        mindshare_change_html = f'<span class="text-danger" data-order="{ms_change:.4f}">{ms_change:.4f}</span>'
                    else:
                        mindshare_change_html = '<span class="text-muted" data-order="0">-</span>'
                else:
                    position_change_html = '<span class="text-muted" data-order="0">-</span>'
                    mindshare_change_html = '<span class="text-muted" data-order="0">-</span>'
                
                table_html += f"""
                    <tr>
                        <td>
                            <div class="d-flex align-items-center">
                                <img src="{row.imageUrl}" alt="{row.name}" class="me-2" style="width:32px;height:32px;border-radius:50%;">
                                <div>
                                    <strong>{row.name}</strong><br>
                                    <small class="text-muted">@{row.username}</small><a href="/wallchain/{projectname}/user/{row.username}" class="user-link" title="유저 분석">🔍</a>
                                </div>
                            </div>
                        </td>
                        <td>{int(prev_position) if prev_position != 9999 else '-'}</td>
                        <td>{int(curr_position) if curr_position != 9999 else '-'}</td>
                        <td>{position_change_html}</td>
                        <td>{row.prev_mindshare:.4f}</td>
                        <td>{row.curr_mindshare:.4f}</td>
                        <td>{mindshare_change_html}</td>
                    </tr>
                    """
            
            table_html += """
                </tbody>
            </table>
            """
        else:
            table_html = "<p>데이터가 없습니다.</p>"
        
        formatted_timestamps = {}
        for ts in timestamps:
            try:
                dt = datetime.strptime(ts, '%Y-%m-%d %H:%M:%S')
                formatted_timestamps[ts] = dt.strftime('%m/%d %H:%M')
            except:
                formatted_timestamps[ts] = ts
        
        timestamp1_display = formatted_timestamps.get(timestamp1, timestamp1)
        timestamp2_display = formatted_timestamps.get(timestamp2, timestamp2)
        
        # 데이터가 있는 timeframe만 필터링
        available_timeframes = []
        for tf in dp.timeframes:
            timestamps_check = dp.get_available_timestamps(tf)
            if timestamps_check:
                available_timeframes.append(tf)
        
        # timeframe 정렬: 7d, 30d, 나머지는 알파벳 순
        def sort_timeframes(tf):
            tf_lower = tf.lower()
            if tf_lower == '7d':
                return (0, tf)
            elif tf_lower == '30d':
                return (1, tf)
            else:
                return (2, tf)
        
        available_timeframes.sort(key=sort_timeframes)
        
        all_wallchain_projects = get_cached_wallchain_projects()
        all_cookie_projects = get_cached_projects()
        grouped_projects = get_grouped_projects()
        grouped_wallchain = get_grouped_wallchain_projects()
        
        return template('leaderboard_wall.html', 
                       project=projectname,
                       lang=lang,
                       display_project_name=dp.project_display_title,
                       current_project=full_project_name,
                       current_page="leaderboard",
                       is_wallchain=True,
                       all_projects=all_cookie_projects,
                       all_wallchain_projects=all_wallchain_projects,
                       grouped_projects=grouped_projects,
                       grouped_wallchain=grouped_wallchain,
                       kaito_projects=get_cached_kaito_projects(),
                       timeframe=timeframe,
                       timeframes=available_timeframes,
                       timestamps=json.dumps(timestamps),
                       formatted_timestamps=json.dumps(formatted_timestamps),
                       timestamp1=timestamp1,
                       timestamp2=timestamp2,
                       timestamp1_display=timestamp1_display,
                       timestamp2_display=timestamp2_display,
                       table_html=table_html)
    except ValueError as e:
        return render_error(str(e), projectname)

@app.route('/wallchain/<projectname>/user/<username>')
def wallchain_user_analysis(projectname, username):
    log_access('wallchain_user', projectname, username)
    lang = get_language()
    
    full_project_name = f"wallchain-{projectname}"
    if full_project_name not in wallchain_instances:
        log_access('invalid_access', projectname)
        return redirect(f'/spaace-en/leaderboard', code=302)
    
    try:
        dp = wallchain_instances[full_project_name]
        
        # 사용 가능한 timeframe 중 실제 데이터가 있는 것을 선택
        timeframe = None
        
        # dp.timeframes에서 데이터가 있는 첫 번째 timeframe 선택
        for tf in dp.timeframes:
            timestamps_check = dp.get_available_timestamps(tf)
            if timestamps_check:
                timeframe = tf
                break
        
        # 그래도 없으면 첫 번째 timeframe 사용
        if not timeframe:
            timeframe = dp.timeframes[0] if dp.timeframes else '7d'
        
        user_info_by_timeframe = {}
        for tf in dp.timeframes:
            user_info_by_timeframe[tf] = dp.get_user_info_by_timeframe(username, tf)
        
        # 선택된 timeframe의 user_info 사용
        user_info = user_info_by_timeframe.get(timeframe, {})
        if not user_info:
            user_info = dp.get_user_info(username)
        
        user_data = dp.get_user_analysis(username)
        
        # 데이터가 있는 timeframe만 필터링
        available_timeframes = []
        for tf in dp.timeframes:
            data = user_data.get(tf, pd.DataFrame())
            if not data.empty:
                available_timeframes.append(tf)
        
        # timeframe 정렬: 7d, 30d, 나머지는 알파벳 순
        def sort_timeframes(tf):
            tf_lower = tf.lower()
            if tf_lower == '7d':
                return (0, tf)
            elif tf_lower == '30d':
                return (1, tf)
            else:
                return (2, tf)
        
        available_timeframes.sort(key=sort_timeframes)
        
        # 데이터가 있는 차트만 생성
        if not available_timeframes:
            user_chart = ""
        else:
            # 언어별 레이블 설정
            if lang == 'ko':
                position_label = '순위'
                mindshare_label = '마인드쉐어'
            else:
                position_label = 'Rank'
                mindshare_label = 'Mindshare'
            
            # subplot_titles를 available_timeframes 기준으로 동적 생성
            subplot_titles_list = [tf.upper() for tf in available_timeframes]
            
            fig = make_subplots(
                rows=len(available_timeframes), cols=1, 
                subplot_titles=tuple(subplot_titles_list),
                vertical_spacing=0.12,
                specs=[[{"secondary_y": True}] for _ in available_timeframes]
            )
            
            for i, tf in enumerate(available_timeframes):
                row = i + 1
                data = user_data.get(tf, pd.DataFrame())
                
                if not data.empty:
                    # 이전 데이터가 있지만 현재 OUT 상태인 경우 더미 데이터 추가
                    if len(data) > 0:
                        latest_timestamp = data['timestamp'].max()
                        # 현재 시점의 데이터 확인
                        timestamps_in_tf = dp.get_available_timestamps(tf)
                        if timestamps_in_tf and len(timestamps_in_tf) > 0:
                            current_timestamp = pd.Timestamp(max(timestamps_in_tf))
                            # 최신 타임스탬프가 현재보다 오래된 경우 (OUT 상태)
                            if latest_timestamp < current_timestamp:
                                # 더미 데이터 추가 (position=9999, mindshare=0)
                                dummy_row = pd.DataFrame({
                                    'timestamp': [current_timestamp],
                                    'position': [9999],
                                    'mindsharePercentage': [0]
                                })
                                data = pd.concat([data, dummy_row], ignore_index=True).sort_values('timestamp')
                    fig.add_trace(
                        go.Scatter(
                            x=data['timestamp'], y=data['position'],
                            mode='lines+markers',
                            name=f'{position_label}',
                            line=dict(color='#FF0000', width=1),
                            marker=dict(size=2, symbol='circle'),
                            showlegend=False,
                        ),
                        row=row, col=1, secondary_y=False
                    )
                    
                    fig.add_trace(
                        go.Scatter(
                            x=data['timestamp'], y=data['mindsharePercentage'],
                            mode='lines+markers',
                            name=f'{mindshare_label}',
                            line=dict(color='#1F77B4', width=1, dash='dot'),
                            marker=dict(size=2, symbol='square'),
                            showlegend=False,
                        ),
                        row=row, col=1, secondary_y=True
                    )
            
            # Y축 설정
            for row_idx in range(1, len(available_timeframes) + 1):
                fig.update_yaxes(
                    title_text=position_label, 
                    autorange="reversed",
                    row=row_idx, col=1, secondary_y=False,
                    gridcolor='lightgray',
                    zeroline=True,
                    fixedrange=True
                )
                
                fig.update_yaxes(
                    title_text=f"{mindshare_label} (%)", 
                    row=row_idx, col=1, secondary_y=True,
                    gridcolor='rgba(0,0,0,0)',
                    fixedrange=True
                )
                
                fig.update_xaxes(
                    row=row_idx, col=1,
                    fixedrange=True
                )
            
            # 차트 높이를 timeframe 개수에 따라 동적 조정
            chart_height = 300 * len(available_timeframes)
            
            fig.update_layout(
                height=chart_height,
                width=None,
                title_text='',
                hovermode="x unified",
                font=dict(size=12, color='#b8b8b8'),
                showlegend=False,
                paper_bgcolor='#2d2d2d',
                plot_bgcolor='#2d2d2d'
            )
            
            # 서브플롯 제목 글꼴 크기 및 위치 조정
            fig.update_annotations(font_size=30)
            fig.update_annotations(x=0.0, xanchor='left')
            
            # Y축 그리드 색상 설정
            for idx in range(1, len(available_timeframes) + 1):
                fig.update_yaxes(gridcolor='#3d3d3d', row=idx, col=1, secondary_y=False)
                fig.update_yaxes(gridcolor='rgba(0,0,0,0)', row=idx, col=1, secondary_y=True)
                fig.update_xaxes(gridcolor='#3d3d3d', row=idx, col=1)
            
            user_chart = pio.to_html(
                fig, 
                full_html=False,
                include_plotlyjs='cdn',
                config={
                    'responsive': True,
                    'staticPlot': False,
                    'displayModeBar': True,
                    'displaylogo': False,
                    'modeBarButtonsToRemove': [
                        'zoom2d', 'pan2d', 'select2d', 'lasso2d',
                        'zoomIn2d', 'zoomOut2d', 'autoscale', 'resetScale2d'
                    ]
                }
            )
        
        all_users = dp.get_all_usernames(timeframe=timeframe)
        all_wallchain_projects = get_cached_wallchain_projects()
        all_cookie_projects = get_cached_projects()
        grouped_projects = get_grouped_projects()
        grouped_wallchain = get_grouped_wallchain_projects()
        
        return template('user_wall.html', 
                       project=projectname,
                       display_project_name=dp.project_display_title,
                       lang=lang,
                       current_project=full_project_name,
                       current_page="user",
                       is_wallchain=True,
                       all_projects=all_cookie_projects,
                       all_wallchain_projects=all_wallchain_projects,
                       grouped_projects=grouped_projects,
                       grouped_wallchain=grouped_wallchain,
                       kaito_projects=get_cached_kaito_projects(),
                       username=username,
                       user_chart=user_chart,
                       user_info=user_info,
                       all_users=json.dumps(all_users),
                       timeframe=timeframe,
                       timeframes=available_timeframes,
                       user_info_by_timeframe=user_info_by_timeframe,
                       json=json)
    except ValueError as e:
        return render_error(str(e), projectname)

# ===================== END WALLCHAIN ROUTES =====================

# ===================== KAITO ROUTES =====================

@app.route('/kaito/<projectname>/')
@app.route('/kaito/<projectname>')
def kaito_index_route(projectname):
    """Kaito 프로젝트 인덱스 페이지"""
    log_access('kaito_index', projectname)
    
    if not kaito_processor:
        return render_error("Kaito 시스템이 초기화되지 않았습니다", projectname)
    
    # 프로젝트 존재 확인
    available_projects = get_cached_kaito_projects()
    if projectname not in available_projects:
        return render_error(f"프로젝트 '{projectname}'를 찾을 수 없습니다", projectname)
    
    # 모든 timeframe에서 unique한 사용자 목록 가져오기
    all_users = []
    try:
        all_users = kaito_processor.get_all_users(projectname)
    except Exception as e:
        print(f"[ERROR] Failed to get users for {projectname}: {e}")
    
    # Navbar variables
    grouped_projects = get_grouped_projects()
    grouped_wallchain = get_grouped_wallchain_projects()
    
    lang = request.get_cookie('lang', 'ko')
    t = {
        'user_analysis': '사용자 분석' if lang == 'ko' else 'User Analysis',
        'leaderboard_analysis': '리더보드 분석' if lang == 'ko' else 'Leaderboard',
        'copy_success': '지갑 주소가 복사되었습니다! 🦈' if lang == 'ko' else 'Wallet address copied! 🦈',
        'click_to_copy': '클릭하여 주소 복사 🦈' if lang == 'ko' else 'Click to copy address🦈'
    }
    
    return template('index_kaito', 
                   projectname=projectname,
                   project=projectname,
                   all_users=all_users,
                   kaito_projects=available_projects,
                   current_page='user',
                   is_kaito=True,
                   lang=lang,
                   t=t,
                   grouped_projects=grouped_projects,
                   grouped_wallchain=grouped_wallchain)


@app.route('/kaito/<projectname>/leaderboard')
def kaito_leaderboard_route(projectname):
    """Kaito 리더보드 비교 페이지"""
    log_access('kaito_leaderboard', projectname)
    
    if not kaito_processor:
        return render_error("Kaito 시스템이 초기화되지 않았습니다", projectname)
    
    timeframe = request.query.get('timeframe', '7D')
    timestamp1 = request.query.get('timestamp1', '')
    timestamp2 = request.query.get('timestamp2', '')
    
    # 프로젝트 존재 확인
    available_projects = get_cached_kaito_projects()
    if projectname not in available_projects:
        return render_error(f"프로젝트 '{projectname}'를 찾을 수 없습니다", projectname)
    
    # 사용 가능한 timeframes
    available_timeframes = kaito_processor.get_available_timeframes(projectname)
    if not available_timeframes:
        return render_error(f"프로젝트 '{projectname}'의 데이터가 없습니다", projectname)
    
    if timeframe not in available_timeframes:
        timeframe = available_timeframes[0]
    
    # 사용 가능한 timestamps 가져오기
    try:
        available_timestamps = kaito_processor.get_available_timestamps(projectname, timeframe)
    except Exception as e:
        print(f"[ERROR] Failed to get timestamps for {projectname}/{timeframe}: {e}")
        return render_error(f"타임프레임 '{timeframe}' 데이터를 불러올 수 없습니다", projectname)
    
    if not available_timestamps:
        return render_error(f"프로젝트 '{projectname}'의 '{timeframe}' 데이터가 없습니다", projectname)
    
    # 기본값 설정
    if not timestamp1 or timestamp1 not in available_timestamps:
        timestamp1 = available_timestamps[-2] if len(available_timestamps) > 1 else available_timestamps[0]
    if not timestamp2 or timestamp2 not in available_timestamps:
        timestamp2 = available_timestamps[-1]
    
    # 리더보드 비교 데이터 가져오기 및 HTML 테이블 생성
    table_html = ""
    if timestamp1 and timestamp2:
        try:
            df = kaito_processor.compare_leaderboards(projectname, timestamp1, timestamp2, timeframe)
            
            if not df.empty:
                lang = request.get_cookie('lang', 'ko')
                
                if lang == 'ko':
                    table_html = """
                    <table id="leaderboardTable" class="table table-striped table-hover">
                        <thead>
                            <tr>
                                <th>사용자</th>
                                <th>이전 순위</th>
                                <th>현재 순위</th>
                                <th>순위 변화</th>
                                <th>이전 마쉐</th>
                                <th>현재 마쉐</th>
                                <th>마쉐 변화</th>
                            </tr>
                        </thead>
                        <tbody>
                    """
                else:
                    table_html = """
                    <table id="leaderboardTable" class="table table-striped table-hover">
                        <thead>
                            <tr>
                                <th>User</th>
                                <th>Pre Rank</th>
                                <th>Cur Rank</th>
                                <th>Rank Change</th>
                                <th>Pre MS</th>
                                <th>Cur MS</th>
                                <th>MS Change</th>
                            </tr>
                        </thead>
                        <tbody>
                    """
                
                for row in df.itertuples():
                    prev_rank = row.prev_rank
                    curr_rank = row.curr_rank
                    
                    # 순위 변화 계산 및 표시
                    if prev_rank == 9999 and curr_rank != 9999:
                        rank_change_html = '<span class="badge bg-success" data-order="0">NEW</span>'
                        ms_change_html = '<span class="badge bg-success" data-order="0">NEW</span>'
                    elif prev_rank != 9999 and curr_rank == 9999:
                        rank_change_html = '<span class="badge bg-secondary" data-order="0">OUT</span>'
                        ms_change_html = '<span class="badge bg-secondary" data-order="0">OUT</span>'
                    elif prev_rank != 9999 and curr_rank != 9999:
                        change = prev_rank - curr_rank
                        if change > 0:
                            rank_change_html = f'<span class="text-success" data-order="{change}">↑ {change}</span>'
                        elif change < 0:
                            rank_change_html = f'<span class="text-danger" data-order="{change}">↓ {abs(change)}</span>'
                        else:
                            rank_change_html = '<span class="text-muted" data-order="0">-</span>'
                        
                        # Mindshare 변화 계산
                        try:
                            prev_ms = float(row.prev_mindshare.rstrip('%'))
                            curr_ms = float(row.curr_mindshare.rstrip('%'))
                            ms_change = curr_ms - prev_ms
                            if ms_change > 0:
                                ms_change_html = f'<span class="text-success" data-order="{ms_change:.2f}">+{ms_change:.2f}%</span>'
                            elif ms_change < 0:
                                ms_change_html = f'<span class="text-danger" data-order="{ms_change:.2f}">{ms_change:.2f}%</span>'
                            else:
                                ms_change_html = '<span class="text-muted" data-order="0">-</span>'
                        except:
                            ms_change_html = '<span class="text-muted" data-order="0">-</span>'
                    else:
                        rank_change_html = '<span class="text-muted" data-order="0">-</span>'
                        ms_change_html = '<span class="text-muted" data-order="0">-</span>'
                    
                    # 프로필 이미지 URL
                    image_url = f"https://pbs.twimg.com/profile_images/{row.imageId}/large.jpg" if row.imageId else ""
                    image_tag = f'<img src="{image_url}" alt="{row.displayName}" class="me-2" style="width:32px;height:32px;border-radius:50%;" onerror="this.style.display=\'none\'">' if image_url else ""
                    
                    table_html += f"""
                        <tr>
                            <td>
                                <div class="d-flex align-items-center">
                                    {image_tag}
                                    <div>
                                        <strong>{row.displayName}</strong><br>
                                        <small class="text-muted">{row.handle}</small><a href="/kaito/{projectname}/user/{row.handle}" class="user-link" title="유저 분석">🔍</a>
                                    </div>
                                </div>
                            </td>
                            <td>{prev_rank if prev_rank != 9999 else '-'}</td>
                            <td>{curr_rank if curr_rank != 9999 else '-'}</td>
                            <td>{rank_change_html}</td>
                            <td>{row.prev_mindshare}</td>
                            <td>{row.curr_mindshare}</td>
                            <td>{ms_change_html}</td>
                        </tr>
                    """
                
                table_html += """
                    </tbody>
                </table>
                """
            else:
                table_html = "<p>데이터가 없습니다.</p>"
        except Exception as e:
            print(f"[ERROR] Failed to compare leaderboards: {e}")
            table_html = "<p>데이터를 불러오는 중 오류가 발생했습니다.</p>"
    
    # timestamp 포맷팅 (YYYY-MM-DD HH:MM 형식)
    formatted_timestamps = {}
    for ts in available_timestamps:
        try:
            # 2026-0102-190000 -> 2026-01-02 19:00
            clean_ts = ts.replace('-', '').replace('_', '')
            dt = pd.to_datetime(clean_ts, format='%Y%m%d%H%M%S')
            formatted_timestamps[ts] = dt.strftime('%Y-%m-%d %H:%M')
        except:
            formatted_timestamps[ts] = ts
    
    timestamp1_display = formatted_timestamps.get(timestamp1, timestamp1)
    timestamp2_display = formatted_timestamps.get(timestamp2, timestamp2)
    
    # Navbar variables
    grouped_projects = get_grouped_projects()
    grouped_wallchain = get_grouped_wallchain_projects()
    
    lang = request.get_cookie('lang', 'ko')
    
    return template('leaderboard_kaito',
                   projectname=projectname,
                   project=projectname,
                   display_project_name=f"🤖 {projectname}",
                   timeframe=timeframe,
                   timeframes=available_timeframes,
                   timestamp1=timestamp1,
                   timestamp2=timestamp2,
                   timestamp1_display=timestamp1_display,
                   timestamp2_display=timestamp2_display,
                   available_timestamps=available_timestamps,
                   timestamps=json.dumps(available_timestamps),
                   formatted_timestamps=json.dumps(formatted_timestamps),
                   table_html=table_html,
                   kaito_projects=available_projects,
                   current_page='leaderboard',
                   is_kaito=True,
                   lang=lang,
                   grouped_projects=grouped_projects,
                   grouped_wallchain=grouped_wallchain)


@app.route('/kaito/<projectname>/user/<handle>')
def kaito_user_route(projectname, handle):
    """Kaito 사용자 분석 페이지"""
    log_access('kaito_user', f"{projectname}/{handle}")
    
    if not kaito_processor:
        return render_error("Kaito 시스템이 초기화되지 않았습니다", projectname)
    
    # 프로젝트 존재 확인
    available_projects = get_cached_kaito_projects()
    if projectname not in available_projects:
        return render_error(f"프로젝트 '{projectname}'를 찾을 수 없습니다", projectname)
    
    # 사용자 기본 정보 가져오기
    user_info = kaito_processor.get_user_info(projectname, handle)
    if not user_info:
        return render_error(f"사용자 '{handle}'를 찾을 수 없습니다", projectname)
    
    # 사용 가능한 timeframe 목록
    available_timeframes = kaito_processor.get_available_timeframes(projectname)
    
    # timeframe별 사용자 데이터 수집 (차트용)
    user_data_by_timeframe = {}
    user_info_by_timeframe = {}
    
    for tf in available_timeframes:
        try:
            df = kaito_processor.get_user_data(projectname, handle, tf)
            if not df.empty:
                user_data_by_timeframe[tf] = df
                # 최신 데이터
                latest_row = df.iloc[-1]
                user_info_by_timeframe[tf] = {
                    'rank': latest_row['rank'],
                    'mindshare': latest_row['mindshare']
                }
        except Exception as e:
            print(f"[ERROR] Failed to get data for {handle} in {tf}: {e}")
    
    # 데이터가 있는 timeframe만 사용
    timeframes_with_data = list(user_data_by_timeframe.keys())
    
    # Plotly 차트 생성 (데이터가 있는 timeframe만)
    if not timeframes_with_data:
        user_chart = ""
    else:
        fig = make_subplots(
            rows=len(timeframes_with_data), cols=1,
            subplot_titles=[f'{tf}' for tf in timeframes_with_data],
            vertical_spacing=0.12,
            specs=[[{"secondary_y": True}] for _ in timeframes_with_data]
        )
        
        for idx, tf in enumerate(timeframes_with_data, 1):
            df = user_data_by_timeframe[tf]
            timestamps = df['timestamp'].tolist()
            ranks = df['rank'].tolist()
            mindshares = df['mindshare'].str.rstrip('%').astype(float).tolist()
            
            # Rank (primary y-axis, reversed)
            fig.add_trace(
                go.Scatter(
                    x=timestamps, 
                    y=ranks, 
                    mode='lines+markers', 
                    name='Rank',
                    line=dict(width=1, color='#FF0000'),
                    marker=dict(size=2, symbol='circle'),
                    showlegend=False
                ),
                row=idx, col=1, secondary_y=False
            )
            
            # Mindshare (secondary y-axis)
            fig.add_trace(
                go.Scatter(
                    x=timestamps, 
                    y=mindshares, 
                    mode='lines+markers', 
                    name='Mindshare',
                    line=dict(width=1, color='#1F77B4', dash='dot'),
                    marker=dict(size=2, symbol='square'),
                    showlegend=False
                ),
                row=idx, col=1, secondary_y=True
            )
            
            # Y축 설정
            fig.update_yaxes(
                title_text="Rank", 
                autorange="reversed",
                row=idx, col=1, secondary_y=False,
                gridcolor='lightgray',
                zeroline=True,
                fixedrange=True
            )
            
            fig.update_yaxes(
                title_text="Mindshare (%)",
                row=idx, col=1, secondary_y=True,
                gridcolor='rgba(0,0,0,0)',
                fixedrange=True
            )
            
            fig.update_xaxes(
                row=idx, col=1,
                fixedrange=True
            )
        
        chart_height = 300 * len(timeframes_with_data)
        
        fig.update_layout(
            height=chart_height,
            width=None,
            hovermode="x unified",
            font=dict(size=12),
            showlegend=False
        )
        
        fig.update_annotations(font_size=30)
        fig.update_annotations(
            x=0.0,
            xanchor='left'
        )
        
        user_chart = pio.to_html(
            fig,
            full_html=False,
            include_plotlyjs='cdn',
            config={
                'responsive': True,
                'staticPlot': False,
                'displayModeBar': True,
                'displaylogo': False,
                'modeBarButtonsToRemove': [
                    'zoom2d',
                    'pan2d',
                    'select2d',
                    'lasso2d',
                    'zoomIn2d',
                    'zoomOut2d',
                    'autoscale',
                    'resetScale2d'
                ]
            }
        )
    
    # Navbar variables
    grouped_projects = get_grouped_projects()
    grouped_wallchain = get_grouped_wallchain_projects()
    
    lang = request.get_cookie('lang', 'ko')
    t = {
        'user_analysis': '사용자 분석' if lang == 'ko' else 'User Analysis',
        'leaderboard_analysis': '리더보드 분석' if lang == 'ko' else 'Leaderboard',
        'copy_success': '지갑 주소가 복사되었습니다! 🦈' if lang == 'ko' else 'Wallet address copied! 🦈',
        'click_to_copy': '클릭하여 주소 복사 🦈' if lang == 'ko' else 'Click to copy address🦈',
        'rank': '순위' if lang == 'ko' else 'Rank',
        'mindshare': '마인드쉐어' if lang == 'ko' else 'Mindshare',
        'followers': '팔로워' if lang == 'ko' else 'Followers',
        'smart_followers': '스마트 팔로워' if lang == 'ko' else 'Smart Followers',
        'chart_title': '순위 및 마인드쉐어 변화 분석' if lang == 'ko' else 'Rank & Mindshare Analysis'
    }
    
    return template('user_kaito',
                   projectname=projectname,
                   project=projectname,
                   handle=handle,
                   user_info=user_info,
                   user_info_by_timeframe=user_info_by_timeframe,
                   timeframes=available_timeframes,
                   user_chart=user_chart,
                   kaito_projects=available_projects,
                   current_page='user',
                   is_kaito=True,
                   lang=lang,
                   t=t,
                   grouped_projects=grouped_projects,
                   grouped_wallchain=grouped_wallchain)

# ===================== END KAITO ROUTES =====================
        
# 404 에러 핸들러 추가 (main.py)
@app.error(404)
def handle_404(error):
    requested_url = request.path
    print(f"[404 ERROR] Requested URL: {requested_url}")  # 디버그용 출력
    log_access('error_page', requested_url)
    
    # URL이 3개 이상의 세그먼트를 가지고 있는지 확인
    url_parts = request.url.split('/')
    if len(url_parts) > 3:
        requested_project = url_parts[3]
    else:
        requested_project = "unknown"
    
    suggestions = [p for p in project_instances.keys() if p.lower() == requested_project.lower()]
    
    if suggestions:
        return redirect(f"/{suggestions[0]}/")
    projectname = "unknown"
    return render_error(f"프로젝트 '{requested_project}'를 찾을 수 없습니다",requested_project)


# 애플리케이션 실행 (Waitress 사용)
from waitress import serve
                
if __name__ == '__main__':
    print("\n" + "="*60)
    print("🦈 SHARKAPP 서버 시작 중...")
    print("="*60)
    
    # 1. 백그라운드 스레드에서 Cookie 프로젝트 초기화
    init_thread = threading.Thread(target=init_projects_on_startup, daemon=True)
    init_thread.start()
    print("📂 Cookie 프로젝트 초기화를 백그라운드에서 진행합니다...")
    
    # 2. 백그라운드 스레드에서 Wallchain 프로젝트 초기화
    wallchain_init_thread = threading.Thread(target=init_wallchain_on_startup, daemon=True)
    wallchain_init_thread.start()
    print("🌊 Wallchain 프로젝트 초기화를 백그라운드에서 진행합니다...")
    
    # 3. Kaito 프로젝트 초기화 및 데이터 로더 시작
    try:
        init_kaito_on_startup()
        start_kaito_data_loader()
        print("🎯 Kaito 프로젝트 초기화 및 데이터 로더 시작...")
    except Exception as e:
        print(f"⚠️ Kaito 초기화 오류: {e}")
    
    # 4. 새 프로젝트 스캔 스레드 시작
    scan_for_new_projects()
    
    # 5. 글로벌 DB 갱신 스케줄러 시작
    schedule_global_updates()
    print("🔄 글로벌 DB 갱신 스케줄러가 시작되었습니다...")
    
    print("\n" + "="*60)
    print("🌐 Waitress Server Running on http://0.0.0.0:8080")
    print("📊 데이터는 백그라운드에서 로드 중입니다...")
    print("="*60 + "\n")
    
    try:
        # Waitress 최적화 설정
        # threads: CPU 코어 수 * 2 (최소 4, 최대 16)
        import multiprocessing
        optimal_threads = max(4, min(16, multiprocessing.cpu_count() * 2))
        
        print(f"⚡ Waitress threads: {optimal_threads}")
        serve(app, 
              host='0.0.0.0', 
              port=8080, 
              threads=optimal_threads,
              channel_timeout=60,  # 요청 타임아웃 60초
              cleanup_interval=10,  # 연결 정리 주기
              asyncore_use_poll=True)  # epoll 사용 (Linux에서 성능 향상)
    except KeyboardInterrupt:
        print("\n[시스템] 종료 중... 모든 프로세스를 강제 종료합니다.")
        import os
        os._exit(0) # 👈 데몬 스레드 무시하고 즉시 종료
