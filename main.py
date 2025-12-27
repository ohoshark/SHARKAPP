from bottle import Bottle, route, run, template, static_file, request, redirect, response, abort
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
base_data_dir = './data/'  # 기본 데이터 디렉토리

# 프로젝트별 DataProcessor 인스턴스 관리
project_instances = {}
# main.py 파일 상단에 로그 파일 경로 설정
LOG_FILE = 'access_log.txt'

# main.py 파일 내 log_access 함수를 아래와 같이 수정

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
    if project_name not in project_instances:
        print(f"[초기화] {project_name} 프로젝트 데이터 로드 시작")
        project_dir = os.path.join(base_data_dir, project_name)
        if not os.path.exists(project_dir):
            raise ValueError(f"Project {project_name} not found")
        
        # DataProcessor 생성 및 초기 데이터 로드
        dp = DataProcessor(project_dir)
        dp.load_data()  # 초기 데이터 강제 로드
        
        project_instances[project_name] = dp  # 수정: dp 인스턴스를 저장
        start_data_loader_thread(project_name)
        print(f"[초기화] {project_name} 데이터 로드 완료")
    return project_instances[project_name]


def start_data_loader_thread(project_name):
    def project_periodic_loader():
        processor = project_instances[project_name]
        while True:
            try:
                new_files = processor.check_for_new_data()
                if new_files:
                    processor.load_data(files_to_load=new_files)
            except Exception as e:
                print(f"[{project_name}] 데이터 로드 오류: {e}")
            time.sleep(30)

    thread = threading.Thread(target=project_periodic_loader, daemon=True)
    thread.start()
    print(f"[{project_name}] 데이터 로더 스레드 시작")

def init_projects_on_startup():
    """서버 시작 시 data 디렉토리 스캔하여 모든 프로젝트 초기화"""
    for project_name in os.listdir(base_data_dir):
        project_path = os.path.join(base_data_dir, project_name)
        if os.path.isdir(project_path):
            try:
                # 프로젝트 인스턴스 강제 생성
                get_data_processor(project_name)
                print(f"[자동 로드] {project_name} 프로젝트 초기화 완료")
            except Exception as e:
                print(f"[오류] {project_name} 프로젝트 로드 실패: {str(e)}")
                
def render_error(error_message, project_name=None):
    try:
        project = project_name or "unknown"
        return template('error.html',
                       current_project=project,
                       project=project,
                       current_page="",
                       error_message=error_message,
                       project_instances=project_instances,
                       json=json)
    except ValueError as e:
        return render_error(str(e), projectname)  # 통일된 에러 렌더링
                
# 프로젝트 하위 경로 처리
@app.route('/<projectname>/static/<filepath:path>')
def serve_project_static(projectname, filepath):
    return static_file(filepath, root='./static')

# 파비콘 요청을 처리하는 라우트 추가
@app.route('/favicon.ico')
def favicon():
    # static_file(파일 이름, root=파일이 있는 디렉터리 경로)
    # 실제 static 폴더 경로에 맞게 수정하세요.
    # print("--- DEBUG: Favicon 라우트 호출됨 ---")
    return static_file('favicon.ico', root='./static')
@app.route('/')
def home_redirect():
    """
    루트 경로 접근 시 DEFAULT_PROJECT로 강제 리디렉션
    """
    log_access('home_redirect', "UNKNOWN")
    # HTTP 상태 코드 302 (Found) 또는 301 (Moved Permanently)와 함께 리디렉션
    return redirect(f'/vooi/leaderboard', code=302)
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
    return redirect(f'/vooi/leaderboard', code=302)
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
    if projectname.lower() == 'favicon.ico':
        # bottle.abort(404)를 사용하여 명시적으로 404 Not Found를 반환합니다.
        abort(404)
    try:
        dp = get_data_processor(projectname)
        timeframe = request.query.get('timeframe', 'TOTAL')

        # 모든 사용자 목록 - username과 displayName 함께 가져옴
        all_users = dp.get_all_usernames(timeframe=timeframe)
        
        return template('index.html', 
                       current_project=projectname,
                       current_page="",
                       project=projectname,
                       all_users=all_users,
                       timeframe=timeframe,
                       timeframes=dp.timeframes)
    except ValueError as e:
        return render_error(str(e), projectname)

@app.route('/<projectname>/leaderboard')
def project_leaderboard(projectname):
    log_access('project_leaderboard', projectname)
    try:
        dp = get_data_processor(projectname)

        timeframe = request.query.get('timeframe', 'TOTAL')
        timestamp1 = request.query.get('timestamp1', '')
        timestamp2 = request.query.get('timestamp2', '')
        # 사용 가능한 타임스탬프 목록
        timestamps = dp.get_available_timestamps(timeframe)
        
        # 타임스탬프가 선택되지 않았거나 유효하지 않은 경우
        if not timestamp1 or timestamp1 not in timestamps:
            timestamp1 = timestamps[-9] if len(timestamps) >= 2 else (timestamps[0] if timestamps else '')
        
        if not timestamp2 or timestamp2 not in timestamps:
            timestamp2 = timestamps[-1] if timestamps else ''
        
        # 리더보드 분석 결과
        compare_data = pd.DataFrame()
        
        if timestamp1 and timestamp2:
            compare_data = dp.compare_leaderboards(timestamp1, timestamp2, timeframe)
        
        # 데이터 테이블을 HTML로 변환
        if not compare_data.empty:
            # 변화량에 화살표 추가하고 스타일 적용
            compare_data['rank_change_display'] = compare_data['rank_change'].apply(
                lambda x: f"{x}" if x > 0 else (f"{x}" )
            )
            compare_data['mindshare_change_display'] = compare_data['mindshare_change'].apply(
                lambda x: f"{x:.4f}" if x > 0 else (f"{x:.4f}" )
            )
            
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
            
            for i, row in enumerate(compare_data.itertuples(), 1):
                # 순위 변화에 따른 스타일 설정
                rank_change_class = "text-success" if row.rank_change > 0 else ("text-danger" if row.rank_change < 0 else "")
                mindshare_change_class = "text-success" if row.mindshare_change > 0 else ("text-danger" if row.mindshare_change < 0 else "")
            

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
                    <td>{row.prev_rank}</td>
                    <td>{row.curr_rank}</td>
                    <td class="{rank_change_class}">{row.rank_change_display}</td>
                    <td>{row.prev_mindshare:.4f}</td>
                    <td>{row.curr_mindshare:.4f}</td>
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
        
        return template('leaderboard.html', 
                       project=projectname,
                       current_project=projectname,
                       current_page="leaderboard",
                       timeframe=timeframe,
                       timeframes=dp.timeframes,
                       timestamps=timestamps,
                       formatted_timestamps=formatted_timestamps,
                       timestamp1=timestamp1,
                       timestamp2=timestamp2,
                       table_html=table_html)
    except ValueError as e:
        return render_error(str(e), projectname)


# 사용자 상세 분석 페이지
@app.route('/<projectname>/user/<username>')
def project_user_analysis(projectname,username):
    log_access('project_leaderboard', projectname, username)
    try:
        dp = get_data_processor(projectname)

        timeframe = request.query.get('timeframe', 'TOTAL')
        
        # 모든 기간의 사용자 데이터 가져오기
        user_data = {}
        for tf in dp.timeframes:  # 7D, 14D, 30D, TOTAL
            user_data[tf] = dp.get_user_history(username, tf)
        
        # 모든 사용자 목록 가져오기 (검색용)
        all_users = dp.get_all_usernames(timeframe=timeframe)
        
        # 선택된 기간의 사용자 정보로 기본 정보 설정
        if user_data[timeframe].empty:
            return template('user.html', 
                           project=projectname,
                           current_project=projectname,
                           current_page="user",
                           username=username, 
                           user_chart="<p>해당 사용자의 데이터가 없습니다.</p>",
                           user_info={},
                           all_users=all_users,
                           timeframe=timeframe,
                           timeframes=dp.timeframes)
        
        # 사용자 기본 정보 (선택된 기간 기준)
        latest = user_data[timeframe].iloc[-1]
        user_info = {
            'displayName': latest.get('displayName', username),
            'followers': latest.get('followers', 0),
            'smartFollowers': latest.get('smartFollowers', 0),
            'rank': latest.get('rank', 'N/A'),
            'snapsPercent': latest.get('snapsPercent', 0),
            'profileImageUrl': latest.get('profileImageUrl', '')
        }
        
        # 4x2 그리드 차트 생성 (각 타임프레임마다 순위와 마인드쉐어 차트 분리)
        fig = make_subplots(
            rows=4, cols=2, 
            subplot_titles=(
                # 첫 번째 행: 7D 차트
                "7일 기준 순위 변화", "14일 기준 순위 변화",
                # 두 번째 행: 7D 마인드쉐어, 14D 마인드쉐어
                "30일 기준 순위 변화", "TOTAL 기준 순위 변화",
                # 세 번째 행: 30D 순위, TOTAL 순위
                "7일 기준 마인드쉐어 변화", "14일 기준 마인드쉐어 변화",
                # 네 번째 행: 30D 마인드쉐어, TOTAL 마인드쉐어
                "30일 기준 마인드쉐어 변화", "TOTAL 기준 마인드쉐어 변화"
            ),
            vertical_spacing=0.08,
            horizontal_spacing=0.05,
            specs=[[{}, {}], [{}, {}], [{}, {}], [{}, {}]]
        )
        
        # 각 타임프레임과 위치 매핑
        tf_positions = {
            '7D': [(1, 1), (3, 1)],  # 순위, 마인드쉐어
            '14D': [(1, 2), (3, 2)],
            '30D': [(2, 1), (4, 1)],
            'TOTAL': [(2, 2), (4, 2)]
        }
        
        # 차트 색상 설정
        rank_color = 'red'
        influence_color = 'blue'
        
        # 각 타임프레임별 차트 생성
        for tf, positions in tf_positions.items():
            rank_pos, influence_pos = positions
            df = user_data[tf]
            
            if not df.empty:
                # 순위 차트
                fig.add_trace(
                    go.Scatter(
                        x=df['timestamp'], 
                        y=df['rank'],
                        mode='lines+markers',
                        name=f'순위({tf})',
                        line=dict(width=2, color=rank_color),
                        showlegend=False
                    ),
                    row=rank_pos[0], col=rank_pos[1]
                )
                
                # 마인드쉐어 지수 차트
                fig.add_trace(
                    go.Scatter(
                        x=df['timestamp'], 
                        y=df['snapsPercent'],
                        mode='lines+markers',
                        name=f'마인드쉐어({tf})',
                        line=dict(width=2, color=influence_color),
                        showlegend=False
                    ),
                    row=influence_pos[0], col=influence_pos[1]
                )
                
                # 순위 차트는 y축 반전 (낮을수록 좋음)
                fig.update_yaxes(autorange="reversed", row=rank_pos[0], col=rank_pos[1])
        
        # 차트 레이아웃 조정
        fig.update_layout(
            height=1200, 
            title_text=f"{user_info['displayName']}의 기간별 순위 및 마인드쉐어 분석",
            hovermode="closest"
        )
        
        # 각 행의 y축 제목 설정
        # for i in range(1, 5):
            # if i == 1 or i == 3:  # 순위 차트
                # fig.update_yaxes(title_text="순위", row=i, col=1)
                # fig.update_yaxes(title_text="순위", row=i, col=2)
            # else:  # 마인드쉐어 차트
                # fig.update_yaxes(title_text="마인드쉐어 지수", row=i, col=1)
                # fig.update_yaxes(title_text="마인드쉐어 지수", row=i, col=2)
        
        user_chart = pio.to_html(fig, full_html=False)
        
        return template('user.html', 
                       project=projectname,
                       current_project=projectname,
                       current_page="user",
                       username=username,
                       user_chart=user_chart,
                       user_info=user_info,
                       all_users=all_users,
                       timeframe=timeframe,
                       timeframes=dp.timeframes)
    except ValueError as e:
        return render_error(str(e), projectname)

# 사용자 비교 페이지
@app.route('/<projectname>/compare')
def project_compare_users(projectname):
    log_access('project_compare', projectname)
    try:
        dp = get_data_processor(projectname)
        timeframe = request.query.get('timeframe', 'TOTAL')
        metric = request.query.get('metric', 'snapsPercent')
        users = request.query.getlist('users')
        
        metrics = {
            'snapsPercent': '마인드쉐어',
            'followers': '팔로워 수',
            'smartFollowers': '주요 팔로워 수',
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
    # 1. 프로젝트 초기화
    init_projects_on_startup()
    print("Waitress Server Running on http://0.0.0.0:8080")
    # Waitress로 서버 구동 시 host='0.0.0.0' 및 threads=4 설정으로 다중 접속을 지원합니다.
    serve(app, host='0.0.0.0', port=8080, threads=50)