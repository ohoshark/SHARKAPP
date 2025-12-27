from bottle import Bottle, route, run, template, static_file, request, redirect
import os
import json
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.io as pio
import threading # ⭐ 추가: 주기적 로딩을 위해 필요
import time # ⭐ 추가: 주기적 로딩을 위해 필요
from datetime import datetime
from data_processor import DataProcessor

# 데이터 로딩을 주기적으로 수행하는 함수 ⭐ 새로 추가
def periodic_data_loader(interval_seconds=10):
    while True:
        try:
            # 1. 새로운 데이터 파일 목록을 가져옵니다.
            new_files = data_processor.check_for_new_data()
            
            if new_files:
                # 발견된 파일 개수 출력
                total_new_files = sum(len(v) for v in new_files.values())
                # print(f"새로운 데이터 파일 {total_new_files}개 발견! 데이터에 추가합니다...")
                
                # 2. 새로운 파일만 로드하도록 load_data 함수에 전달
                data_processor.load_data(files_to_load=new_files) 
                # print("데이터 추가 로드 완료.")
            # else:
                # print("새로운 데이터가 없습니다. 대기합니다.")
                
        except Exception as e:
            print(f"주기적 데이터 로드 중 오류 발생: {e}")
            
        # 지정된 시간(초)만큼 대기합니다. (5분)
        time.sleep(interval_seconds) 


# 애플리케이션 초기화
app = Bottle()
data_dir = './data/vooi/'  # 데이터 폴더 경로 설정
data_processor = DataProcessor(data_dir)
data_processor.load_data() # 초기 로드

# 애플리케이션 실행 전, 데이터 로더 스레드 시작 ⭐ 추가
# interval_seconds: 5분(300초)마다 새로운 파일 확인
loader_thread = threading.Thread(target=periodic_data_loader, args=(30,))
# 서버 종료 시 스레드도 함께 종료되도록 데몬 설정
loader_thread.daemon = True 
loader_thread.start()
print("주기적 데이터 로더 스레드가 시작되었습니다. (30초 간격)")


# 정적 파일 서비스
@app.route('/static/<filepath:path>')
def serve_static(filepath):
    return static_file(filepath, root='./static')

# 메인 대시보드
@app.route('/')
def index():
    timeframe = request.query.get('timeframe', 'TOTAL')
    top_users = data_processor.get_top_users(timeframe=timeframe, n=20)
    
    # 상위 사용자 시각화
    if not top_users.empty:
        fig = px.bar(
            top_users, 
            y='displayName', 
            x='snapsPercent', 
            title=f'상위 20명 스내퍼 (기간: {timeframe})',
            labels={'snapsPercent': '마인드쉐어', 'displayName': '사용자 이름'},
            color='snapsPercent',
            orientation='h',
            height=800
        )
        fig.update_layout(yaxis={'categoryorder': 'total ascending'})
        top_users_chart = pio.to_html(fig, full_html=False)
    else:
        top_users_chart = "<p>데이터가 없습니다.</p>"
    
    # 시간에 따른 평균 마인드쉐어 추이
    trend_data = data_processor.get_trend_data(timeframe=timeframe)
    if not trend_data.empty:
        fig = px.line(
            trend_data, 
            x='timestamp', 
            y='snapsPercent',
            title=f'평균 마인드쉐어 추이 (기간: {timeframe})',
            labels={'snapsPercent': '평균 마인드쉐어', 'timestamp': '시간'}
        )
        trend_chart = pio.to_html(fig, full_html=False)
    else:
        trend_chart = "<p>트렌드 데이터가 없습니다.</p>"
    
    # 모든 사용자 목록 - username과 displayName 함께 가져옴
    all_users = data_processor.get_all_usernames(timeframe=timeframe)
    
    return template('index.html', 
                   top_users_chart=top_users_chart,
                   trend_chart=trend_chart,
                   all_users=all_users,
                   timeframe=timeframe,
                   timeframes=data_processor.timeframes)

@app.route('/leaderboard')
def leaderboard_comparison():
    timeframe = request.query.get('timeframe', 'TOTAL')
    timestamp1 = request.query.get('timestamp1', '')
    timestamp2 = request.query.get('timestamp2', '')
    # 사용 가능한 타임스탬프 목록
    timestamps = data_processor.get_available_timestamps(timeframe)
    
    # 타임스탬프가 선택되지 않았거나 유효하지 않은 경우
    if not timestamp1 or timestamp1 not in timestamps:
        timestamp1 = timestamps[-9] if len(timestamps) >= 2 else (timestamps[0] if timestamps else '')
    
    if not timestamp2 or timestamp2 not in timestamps:
        timestamp2 = timestamps[-1] if timestamps else ''
    
    # 리더보드 분석 결과
    compare_data = pd.DataFrame()
    
    if timestamp1 and timestamp2:
        compare_data = data_processor.compare_leaderboards(timestamp1, timestamp2, timeframe)
    
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
                            <small class="text-muted">@{row.username}</small><a href="./user/{row.username}" class="user-link" title="유저 분석">🔍</a>
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
                   timeframe=timeframe,
                   timeframes=data_processor.timeframes,
                   timestamps=timestamps,
                   formatted_timestamps=formatted_timestamps,
                   timestamp1=timestamp1,
                   timestamp2=timestamp2,
                   table_html=table_html)



# 사용자 상세 분석 페이지
@app.route('/user/<username>')
def user_analysis(username):
    timeframe = request.query.get('timeframe', 'TOTAL')
    
    # 모든 기간의 사용자 데이터 가져오기
    user_data = {}
    for tf in data_processor.timeframes:  # 7D, 14D, 30D, TOTAL
        user_data[tf] = data_processor.get_user_history(username, tf)
    
    # 모든 사용자 목록 가져오기 (검색용)
    all_users = data_processor.get_all_usernames(timeframe=timeframe)
    
    # 선택된 기간의 사용자 정보로 기본 정보 설정
    if user_data[timeframe].empty:
        return template('user.html', 
                       username=username, 
                       user_chart="<p>해당 사용자의 데이터가 없습니다.</p>",
                       user_info={},
                       all_users=all_users,
                       timeframe=timeframe,
                       timeframes=data_processor.timeframes)
    
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
                   username=username,
                   user_chart=user_chart,
                   user_info=user_info,
                   all_users=all_users,
                   timeframe=timeframe,
                   timeframes=data_processor.timeframes)


# 사용자 비교 페이지
@app.route('/compare')
def compare_users():
    timeframe = request.query.get('timeframe', 'TOTAL')
    metric = request.query.get('metric', 'snapsPercent')
    users = request.query.getlist('users')
    
    metrics = {
        'snapsPercent': '마인드쉐어',
        'followers': '팔로워 수',
        'smartFollowers': '주요 팔로워 수',
        'rank': '순위'
    }
    
    all_users = data_processor.get_all_usernames(timeframe=timeframe)
    
    if users:
        user_comparison = data_processor.get_user_comparison(users, timeframe, metric)
        
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
                   comparison_chart=comparison_chart,
                   all_users=all_users,
                   selected_users=users,
                   timeframe=timeframe,
                   metric=metric,
                   metrics=metrics,
                   timeframes=data_processor.timeframes)

# 애플리케이션 실행 (Waitress 사용)
from waitress import serve

if __name__ == '__main__':
    print("Waitress Server Running on http://0.0.0.0:8080")
    # Waitress로 서버 구동 시 host='0.0.0.0' 및 threads=4 설정으로 다중 접속을 지원합니다.
    serve(app, host='0.0.0.0', port=8080, threads=50)