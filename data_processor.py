import os
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
        self.data = {}
        self.user_data = {}
        self.trends = {}
        self.latest_file = {} # ⭐ 추가: 가장 최근 로드된 파일명을 저장

    def load_data(self, files_to_load=None):
        """
        [수정됨] files_to_load가 None이면 모든 파일을 로드하고, 
        특정 파일 목록이 주어지면 해당 파일만 로드합니다.
        """
        if files_to_load is None:
            # 초기 로딩 시 모든 파일을 로드
            files_to_load = defaultdict(list)
            for timeframe in self.timeframes:
                path = os.path.join(self.data_dir, timeframe)
                # print(path)
                files_to_load[timeframe] = sorted(glob.glob(os.path.join(path, "*.json")))
                
            # 초기 로딩 시 기존 데이터 프레임 초기화
            self.data = {}
        
        new_data_loaded = False
        
        for timeframe in self.timeframes:
            if timeframe not in files_to_load or not files_to_load[timeframe]:
                continue

            timeframe_data = []
            
            # files_to_load에 있는 파일만 처리
            for file in files_to_load[timeframe]:
                try:
                    # 파일명에서 타임스탬프 추출
                    filename = os.path.basename(file)
                    timestamp = datetime.strptime(filename.split('_')[0] + '_' + filename.split('_')[1], 
                                                '%Y%m%d_%H%M%S')
                    
                    with open(file, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        
                    # 데이터에 타임스탬프 추가
                    if 'result' in data and 'data' in data['result'] and 'json' in data['result']['data']:
                        snaps_data = data['result']['data']['json'].get('snaps', [])
                        for snap in snaps_data:
                            snap['timestamp'] = timestamp.strftime('%Y-%m-%d %H:%M:%S')
                            timeframe_data.append(snap)
                        
                        # ⭐ 로드 성공 시 최신 파일명 업데이트
                        self.latest_file[timeframe] = filename
                        new_data_loaded = True
                        
                except Exception as e:
                    print(f"Error loading {file}: {e}")
            
            # 새로 로드한 데이터가 있다면, 기존 데이터프레임에 추가 (병합)
            new_df = pd.DataFrame(timeframe_data)
            if timeframe in self.data and not self.data[timeframe].empty and not new_df.empty:
                # 기존 데이터와 새로 로드된 데이터를 합칩니다.
                self.data[timeframe] = pd.concat([self.data[timeframe], new_df], ignore_index=True)
                # 합친 후 중복 제거 및 정렬
                self.data[timeframe].drop_duplicates(subset=['username', 'timestamp'], keep='last', inplace=True)
                self.data[timeframe].sort_values('timestamp', inplace=True)
                
            elif not new_df.empty:
                # 최초 로드 또는 기존 데이터가 없으면 새 데이터프레임으로 설정
                self.data[timeframe] = new_df
            
        if new_data_loaded:
            # 데이터가 갱신되었으면 사용자별/트렌드 데이터도 갱신
            self.process_user_data()
            self.process_trend_data()
            
        return self.data

    def check_for_new_data(self):
        """
        [새로 추가] 새로운 데이터 파일 목록을 반환합니다.
        새로운 파일이 없으면 빈 딕셔너리를 반환합니다.
        """
        new_files_to_load = defaultdict(list)
        is_new_data_found = False
        
        for timeframe in self.timeframes:
            path = os.path.join(self.data_dir, timeframe)
            all_files = sorted(glob.glob(os.path.join(path, "*.json")))
            
            current_latest = self.latest_file.get(timeframe, '')
            
            # 현재 로드된 파일보다 이름순(시간순)으로 뒤에 있는 파일만 필터링
            for file in all_files:
                filename = os.path.basename(file)
                if filename > current_latest:
                    new_files_to_load[timeframe].append(file)
                    is_new_data_found = True
            
        return new_files_to_load if is_new_data_found else {}

    def process_user_data(self):
        """사용자별 데이터를 처리합니다"""
        for timeframe, df in self.data.items():
            if not df.empty and 'username' in df.columns:
                # 사용자별 데이터 그룹화
                self.user_data[timeframe] = {}
                for username in df['username'].unique():
                    user_df = df[df['username'] == username].sort_values('timestamp')
                    self.user_data[timeframe][username] = user_df
    
    def process_trend_data(self):
        """시간에 따른 트렌드 데이터를 처리합니다"""
        for timeframe, df in self.data.items():
            if not df.empty and 'timestamp' in df.columns:
                # 타임스탬프별 통계
                self.trends[timeframe] = df.groupby('timestamp').agg({
                    'snapsPercent': 'mean',
                    'followers': 'mean',
                    'smartFollowers': 'mean',
                    'rank': 'min'  # 최고 순위
                }).reset_index()
                
    def get_top_users(self, timeframe='TOTAL', n=10, metric='snapsPercent'):
        """특정 지표 기준으로 상위 사용자를 반환합니다"""
        if timeframe in self.data and not self.data[timeframe].empty:
            # 가장 최근 타임스탬프의 데이터만 선택
            latest = self.data[timeframe]['timestamp'].max()
            latest_df = self.data[timeframe][self.data[timeframe]['timestamp'] == latest]
            
            # 지표별 정렬
            if metric in latest_df.columns:
                return latest_df.sort_values(by=metric, ascending=False).head(n)
        
        return pd.DataFrame()
    
    def get_user_history(self, username, timeframe='TOTAL'):
        """특정 사용자의 시간에 따른 데이터를 반환합니다"""
        if timeframe in self.user_data and username in self.user_data[timeframe]:
            history = self.user_data[timeframe][username]
             #데이터가 많을 경우 샘플링 (시각화 성능 향상용)
            # if len(history) > 100:
                # return history.iloc[::len(history)//100]
            return history
        return pd.DataFrame()
    # data_processor.py 파일 내 DataProcessor 클래스에 추가

    def get_all_users(self):
        """
        검색 기능을 위해 모든 사용자의 username과 displayName을 반환합니다.
        """
        # 가장 최근의 'TOTAL' 데이터프레임을 사용합니다.
        if 'TOTAL' in self.data and not self.data['TOTAL'].empty:
            df = self.data['TOTAL']
            latest = df['timestamp'].max()
            
            # 가장 최근 데이터만 필터링
            latest_df = df[df['timestamp'] == latest]
            
            # username과 displayName만 추출하여 딕셔너리 리스트 형태로 반환
            return latest_df[['username', 'displayName']].to_dict('records')
        
        return []
    # data_processor.py 파일 내 DataProcessor 클래스에 추가
    def get_user_info_by_timeframe(self, username, timeframe='TOTAL'):
        """
        [추가됨] 특정 타임프레임의 가장 최근 사용자 정보를 반환합니다.
        (user.html의 통계 카드 업데이트에 사용)
        """
        # 지정된 timeframe의 가장 최근 데이터를 사용
        if timeframe in self.data and not self.data[timeframe].empty:
            # 가장 최근 타임스탬프를 찾습니다.
            latest = self.data[timeframe]['timestamp'].max()
            
            # 해당 타임스탬프에서 특정 사용자의 데이터를 필터링합니다.
            user_row = self.data[timeframe][
                (self.data[timeframe]['timestamp'] == latest) & 
                (self.data[timeframe]['username'] == username)
            ]
            
            if not user_row.empty:
                # 첫 번째 일치하는 행의 데이터를 딕셔너리로 반환 (user_info 역할을 수행)
                return user_row.iloc[0].to_dict()
        
        # 데이터를 찾지 못하면 기본 정보를 반환하여 오류 방지
        # displayName을 찾기 위해 기존 get_user_info()를 한 번 더 호출할 수 있습니다.
        # 기존 get_user_info()는 TOTAL에서 displayName을 찾아줌
        default_info = self.get_user_info(username) 
        # 선택된 timeframe에 rank, mindshare 관련 정보는 없음을 표시
        return {
            'username': username, 
            'displayName': default_info['displayName'],
            'rank': '-',
            'cSnapsPercentRank': '-',
            'snapsPercent': 0.0,
            'cSnapsPercent': 0.0,
            'followers': default_info.get('followers', 0),
            'smartFollowers': default_info.get('smartFollowers', 0),
        }
    def get_user_info(self, username):
        """
        특정 사용자의 displayName을 포함한 최신 정보를 반환합니다.
        (main.py에서 user_info['displayName']을 참조하기 위해 필요)
        """
        # 'TOTAL' timeframe의 가장 최근 데이터를 사용
        if 'TOTAL' in self.data and not self.data['TOTAL'].empty:
            # 가장 최근 타임스탬프를 찾습니다.
            latest = self.data['TOTAL']['timestamp'].max()
            
            # 해당 타임스탬프에서 특정 사용자의 데이터를 필터링합니다.
            user_row = self.data['TOTAL'][
                (self.data['TOTAL']['timestamp'] == latest) & 
                (self.data['TOTAL']['username'] == username)
            ]
            
            if not user_row.empty:
                # 첫 번째 일치하는 행의 데이터를 딕셔너리로 반환 (user_info 역할을 수행)
                return user_row.iloc[0].to_dict()
        
        # 데이터를 찾지 못하면 기본 정보를 반환하여 오류 방지
        return {'username': username, 'displayName': username}
    def get_user_analysis(self, username):
        """
        주요 타임프레임별로 사용자 데이터를 가져와 딕셔너리로 반환합니다.
        main.py의 차트 생성 루프를 위해 필요합니다.
        """
        analysis_data = {}
        for tf in self.timeframes:
            # 기존 get_user_history 함수를 사용하여 각 기간별 데이터를 가져옵니다.
            analysis_data[tf] = self.get_user_history(username, tf)
        return analysis_data

    def get_trend_data(self, timeframe='TOTAL', metric='snapsPercent'):
        """시간에 따른 트렌드 데이터를 반환합니다"""
        if timeframe in self.trends and metric in self.trends[timeframe].columns:
            return self.trends[timeframe][['timestamp', metric]]
        return pd.DataFrame()
    
    def get_user_comparison(self, usernames, timeframe='TOTAL', metric='snapsPercent'):
        """여러 사용자를 비교하기 위한 데이터를 반환합니다"""
        result = {}
        for username in usernames:
            user_data = self.get_user_history(username, timeframe)
            if not user_data.empty and metric in user_data.columns:
                result[username] = user_data[['timestamp', metric]]
        
        return result
    
    def get_all_usernames(self, timeframe='TOTAL'):
        """특정 타임프레임의 모든 사용자 목록을 반환합니다 (username과 displayName 포함)"""
        if timeframe in self.data and not self.data[timeframe].empty:
            # 가장 최근 타임스탬프의 데이터만 선택
            latest = self.data[timeframe]['timestamp'].max()
            latest_df = self.data[timeframe][self.data[timeframe]['timestamp'] == latest]
            
            # username과 displayName을 함께 반환
            return latest_df[['username', 'displayName']].to_dict('records')
        return []

    def get_available_timestamps(self, timeframe='TOTAL'):
        """특정 타임프레임에서 사용 가능한 타임스탬프 목록을 반환합니다."""
        if timeframe in self.data and not self.data[timeframe].empty:
            return sorted(self.data[timeframe]['timestamp'].unique())
        return []

    def get_leaderboard_at_timestamp(self, timestamp, timeframe='TOTAL'):
        """특정 타임스탬프에서의 리더보드 데이터를 반환합니다."""
        if timeframe in self.data and not self.data[timeframe].empty:
            return self.data[timeframe][self.data[timeframe]['timestamp'] == timestamp].copy()
        return pd.DataFrame()


    def compare_leaderboards(self, timestamp1, timestamp2, timeframe='TOTAL', metric='snapsPercent'):

        # ⭐ [추가] 지표 및 순위 컬럼 이름 정의 ⭐
        if metric == 'snapsPercent':
            rank_col = 'rank'
            mindshare_col = 'snapsPercent'
            mindshare_change_col = 'mindshare_change'
        elif metric == 'cSnapsPercent':
            # cSnapsPercent를 위한 순위와 지표 컬럼 이름 사용
            rank_col = 'cSnapsPercentRank' 
            mindshare_col = 'cSnapsPercent'
            mindshare_change_col = 'c_mindshare_change' # 마쉐 변화와 구분하기 위해 이름 변경
        else:
            raise ValueError(f"Unsupported metric: {metric}")
            
        df1 = self.get_leaderboard_at_timestamp(timestamp1, timeframe)
        df2 = self.get_leaderboard_at_timestamp(timestamp2, timeframe)
        
        if df1.empty and df2.empty:
            return pd.DataFrame()

        # 열 이름 변경 (동적)
        df1 = df1.rename(columns={
            rank_col: 'prev_rank', 
            mindshare_col: 'prev_mindshare', # mindshare_col이 snapsPercent 또는 cSnapsPercent가 됨
            'profileImageUrl': 'prev_profileImageUrl'
        })
        
        df2 = df2.rename(columns={
            rank_col: 'curr_rank', 
            mindshare_col: 'curr_mindshare', # mindshare_col이 snapsPercent 또는 cSnapsPercent가 됨
            'profileImageUrl': 'curr_profileImageUrl'
        })
        
        # outer merge
        compare_data = pd.merge(
            df1, 
            df2, 
            on='username', 
            how='outer',
            suffixes=('_prev', '_curr') 
        )
        
        # displayName 갱신 및 정리
        compare_data['displayName'] = compare_data['displayName_curr'].combine_first(
            compare_data['displayName_prev']
        ).fillna('')
        
        compare_data.drop(columns=['displayName_prev', 'displayName_curr'], inplace=True, errors='ignore')

        # 0/999로 처리할 열 정의
        # ⭐ [수정] mindshare 컬럼명을 동적으로 변경 ⭐
        fillna_mindshare_cols = ['prev_mindshare', 'curr_mindshare'] 
        
        fillna_prev_rank_cols = ['prev_rank']
        fillna_curr_rank_cols = ['curr_rank']
        rank_cols = ['prev_rank', 'curr_rank'] 

        # 결측치(NaN)를 채우기
        compare_data.fillna({col: 0 for col in fillna_mindshare_cols}, inplace=True)
        compare_data.fillna({col: 999 for col in fillna_prev_rank_cols}, inplace=True)
        compare_data.fillna({col: 999 for col in fillna_curr_rank_cols}, inplace=True)

        # rank 관련 열을 정수형(int)으로 변환
        for col in rank_cols:
            compare_data[col] = compare_data[col].astype(int)
        
        # --- 변화량 계산 ---
        compare_data['rank_change'] = compare_data['prev_rank'] - compare_data['curr_rank']
        
        condition = abs(compare_data['rank_change']) > 149
        
        # rank_change의 절댓값이 149를 초과하는 경우 0으로 덮어씁니다.
        compare_data['rank_change'] = np.where(
            abs(compare_data['rank_change']) > 149,
            0,
            compare_data['rank_change']
        ).astype(int)
        
        # ⭐ [수정] 마인드쉐어 변화 컬럼 이름도 동적으로 설정 ⭐
        compare_data[mindshare_change_col] = compare_data['curr_mindshare'] - compare_data['prev_mindshare']
        
        # 2. 조건에 따라 두 열 모두 0으로 설정
        compare_data.loc[condition, mindshare_change_col] = 0

        # --- 프로필 이미지 URL 정리 ---
        compare_data['profileImageUrl'] = compare_data['curr_profileImageUrl'].combine_first(
            compare_data['prev_profileImageUrl']
        ).fillna('')
        
        # ⭐ [수정] 최종 결과 열 선택 시 동적 마쉐 변화 컬럼 포함 ⭐
        result_cols = [
            'username', 'displayName', 'profileImageUrl',
            'prev_rank', 'curr_rank', 'rank_change',
            'prev_mindshare', 'curr_mindshare', mindshare_change_col # ⭐ 동적 컬럼명 ⭐
        ]
        
        # ⭐⭐ 경고 해결 핵심: .copy()를 추가하여 명시적인 복사본을 만듭니다. ⭐⭐
        result = compare_data[result_cols].copy() 

        # 순위 변화 기준으로 정렬
        result.sort_values('rank_change', ascending=False, inplace=True) 
        
        # ⭐ [추가] 컬럼 이름을 일관되게 'mindshare_change'로 통일하여 main.py에서 참조하기 쉽도록 함 ⭐
        # main.py에서 `if metric == 'cSnapsPercent'` 분기 없이 통일된 이름으로 참조할 수 있도록 조정
        # 이 부분은 `main.py`의 구현을 단순화하기 위한 트릭입니다.
        if metric == 'cSnapsPercent':
             result.rename(columns={'prev_mindshare': 'prev_c_mindshare', 
                                   'curr_mindshare': 'curr_c_mindshare'}, 
                                    inplace=True)
        else:
             # 기본 mindshare인 경우에도 일관된 이름으로 유지
             result.rename(columns={'prev_mindshare': 'prev_mindshare', 
                                   'curr_mindshare': 'curr_mindshare'}, 
                                    inplace=True)

        return result