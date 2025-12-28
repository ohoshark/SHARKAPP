from bottle import Bottle, route, run, template, static_file, request, redirect, response, abort, TEMPLATE_PATH
from concurrent.futures import ThreadPoolExecutor  # 상단에 추가
import os
import json
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.io as pio
import threading
import time
from datetime import datetime
from data_processor import DataProcessor

app = Bottle()

# 템플릿 경로 설정 (views 폴더와 루트 폴더 모두 포함)
TEMPLATE_PATH.insert(0, './views/')
TEMPLATE_PATH.insert(0, './')

base_data_dir = './data/'  # 기본 데이터 디렉토리

# 프로젝트별 DataProcessor 인스턴스 관리
project_instances = {}
# main.py 파일 상단에 로그 파일 경로 설정
LOG_FILE = 'access_log.txt'

# main.py 파일 내 log_access 함수를 아래와 같이 수정
PROJECT_CACHE = {"list": [], "last_updated": 0}
CACHE_INTERVAL = 300  # 5분마다 갱신 (필요에 따라 조절)

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
    
    try:
        with open(LOG_FILE, 'a', encoding='utf-8') as f:
            f.write(log_message)
    except Exception as e:
        print(f"[ERROR] 로그 파일 쓰기 실패: {e}")
        
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
                
def render_error(error_message, project_name=None):
    try:
        project = project_name or "unknown"
        all_projects = get_cached_projects()
        lang = get_language()  # 현재 설정된 언어 가져오기
        return template('error.html',
                       current_project=project,
                       project=project,
                       current_page="",
                       lang=lang,
                       all_projects=all_projects,
                       error_message=error_message,
                       project_instances=project_instances,
                       json=json)
    except ValueError as e:
        return render_error(str(e), projectname)  # 통일된 에러 렌더링
                
# 프로젝트 하위 경로 처리
@app.route('/<projectname>/static/<filepath:path>')
def serve_project_static(projectname, filepath):
    return static_file(filepath, root='./static')
@app.route('/static/<filename:path>')
def send_static(filename):
    return static_file(filename, root='./static') # 또는 이미지가 저장된 폴더명
# robots.txt 요청을 처리하는 라우트 추가
@app.route('/robots.txt')
def robots():
    return static_file('robots.txt', root='./static')
# 파비콘 요청을 처리하는 라우트 추가
@app.route('/favicon.ico')
def favicon():
    # static_file(파일 이름, root=파일이 있는 디렉터리 경로)
    # 실제 static 폴더 경로에 맞게 수정하세요.
    # print("--- DEBUG: Favicon 라우트 호출됨 ---")
    return static_file('favicon.ico', root='./static')

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
        # 모든 사용자 목록 - username과 displayName 함께 가져옴
        all_users = dp.get_all_usernames(timeframe=timeframe)
        all_projects = get_cached_projects()
        return template('index.html', 
                       current_project=projectname,
                       display_project_name=display_project_name,
                       lang=lang,
                       current_page="",
                       project=projectname,
                       all_projects=all_projects,
                       all_users=all_users,
                       timeframe=timeframe,
                       timeframes=dp.timeframes)
    except ValueError as e:
        return render_error(str(e), projectname)

@app.route('/<projectname>/leaderboard')
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
                # 순위 변화에 따른 스타일 설정
                rank_change_class = "text-success" if row.rank_change > 0 else ("text-danger" if row.rank_change < 0 else "")
                mindshare_change_value = getattr(row, mindshare_change_col)
                mindshare_change_class = "text-success" if mindshare_change_value > 0 else ("text-danger" if mindshare_change_value < 0 else "")
            
                # ⭐⭐⭐ [핵심 수정] 이전/현재 마쉐 값을 동적으로 참조하여 변수 정의 (추가/복구) ⭐⭐⭐
                prev_mindshare_value = getattr(row, prev_mindshare_col)
                curr_mindshare_value = getattr(row, curr_mindshare_col)
                # ⭐⭐⭐ 수정/복구 끝 ⭐⭐⭐
                table_html += f"""
                    <tr>
                        <td>
                            <div class="d-flex align-items-center">
                                <img src="{row.profileImageUrl}" alt="{row.displayName}" class="me-2" style="width:32px;height:32px;border-radius:50%;">
                                <div>
                                    <strong>{row.displayName}</strong><br>
                                    <small class="text-muted">@{row.username}</small><a href="/{projectname}/user/{row.username}" class="user-link" title="유저 분석">🔍</a>
                                </div>
                            </div>
                        </td>
                        <td>{int(row.prev_rank)}</td>
                        <td>{int(row.curr_rank)}</td>
                        <td class="{rank_change_class}">{int(row.rank_change)}</td>
                        <td>{prev_mindshare_value:.4f}</td>
                        <td>{curr_mindshare_value:.4f}</td>
                        <td class="{mindshare_change_class}">{row.mindshare_change_display}</td>
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
        # print(user_data)
        # ⭐⭐⭐ [수정 1] 4행 1열 서브플롯 생성 및 보조 Y축 설정 ⭐⭐⭐
        # 4개 기간별 차트를 세로로 나열
        fig = make_subplots(
            rows=4, cols=1, 
            subplot_titles=('7D', '14D', '30D', 'TOTAL'),
            vertical_spacing=0.12, # 차트 간 간격 조정
            # 모든 서브플롯에 보조 Y축(secondary_y) 활성화
            specs=[[{"secondary_y": True}], [{"secondary_y": True}], [{"secondary_y": True}], [{"secondary_y": True}]]
        )
        
        # ⭐⭐⭐ [수정 2] 차트 그리기 루프: 순위/마쉐를 하나의 서브플롯에 추가 ⭐⭐⭐
        # dp.timeframes = ['7D', '14D', '30D', 'TOTAL'] 순서를 따름
        for i, tf in enumerate(dp.timeframes):
            row_num = i + 1 # 1부터 4까지의 행 번호
            df = user_data[tf]
            
            if not df.empty:
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
                # ⭐ [추가 3] X축 설정: X축 드래그/줌 비활성화 ⭐
                fig.update_xaxes(
                    row=row_num, col=1, 
                    fixedrange=True
                )
                
            # ⭐⭐⭐ [수정 3] 레이아웃 및 범례 설정 ⭐⭐⭐
            fig.update_layout(
                # 4개의 차트가 세로로 나열되므로 높이 조정
                height=1200, 
                width=None, # 클라이언트 CSS에 너비를 맡김
                title_text= title,
                hovermode="x unified", # 툴팁을 통합하여 가독성 향상
                font=dict(size=12),
                # dragmode="hovermode",
                showlegend=False
                # 범례를 차트 하단 중앙에 배치하여 공간 절약 및 가독성 확보
                # legend=dict(
                    # orientation="h", 
                    # yanchor="bottom", 
                    # y=-0.1, 
                    # xanchor="center", 
                    # x=0.5,
                    # bgcolor="rgba(255, 255, 255, 0.7)",
                    # bordercolor="lightgray",
                    # borderwidth=1
                # )
            )
            
            # 서브플롯 제목 글꼴 크기 조정
            fig.update_annotations(font_size=30)
            fig.update_annotations(
                    # 1. 제목의 가로 위치를 서브플롯의 맨 왼쪽(0.0)으로 설정
                    x=0.0, 
                    # 2. 제목 텍스트의 '왼쪽 끝'을 위에서 지정한 x=0.0 좌표에 고정
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
            
            display_project_name = dp.project_name
            # {'ko': '🇰🇷', 'en': '🌐', 'zh': '🇨🇳'}
            display_project_name = get_flag(dp.lang) +" " + display_project_name

        except AttributeError:
            # 안전을 위해 DataProcessor에 해당 메서드가 없을 경우 빈 리스트로 처리
            all_users = []
            all_projects = []
        return template('user.html', 
                       project=projectname,
                       display_project_name=display_project_name,
                       lang=lang,
                       current_project=projectname,
                       current_page="user",
                       all_projects=all_projects,
                       username=username,
                       user_chart=user_chart,
                       user_info=user_info,
                       all_users=json.dumps(all_users), # JSON 문자열로 변환
                       timeframe=timeframe,
                       metric=metric, 
                       timeframes=dp.timeframes,
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
        
# 404 에러 핸들러 추가 (main.py)
@app.error(404)
def handle_404(error):
    log_access('error_page', "UNKNOWN")
    requested_project = request.url.split('/')[3]
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
    
    # 1. 백그라운드 스레드에서 프로젝트 초기화
    init_thread = threading.Thread(target=init_projects_on_startup, daemon=True)
    init_thread.start()
    print("📂 프로젝트 초기화를 백그라운드에서 진행합니다...")
    
    print("\n" + "="*60)
    print("🌐 Waitress Server Running on http://0.0.0.0:8080")
    print("📊 데이터는 백그라운드에서 로드 중입니다...")
    print("="*60 + "\n")
    
    try:
        # Waitress로 서버 구동 시 host='0.0.0.0' 및 threads=50 설정으로 다중 접속을 지원합니다.
        serve(app, host='0.0.0.0', port=8080, threads=50)
    except KeyboardInterrupt:
        print("\n[시스템] 종료 중... 모든 프로세스를 강제 종료합니다.")
        import os
        os._exit(0) # 👈 데몬 스레드 무시하고 즉시 종료
