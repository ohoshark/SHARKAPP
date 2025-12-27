import os
import orjson  # json 대신 orjson 임포트
import json
import pandas as pd
import pickle  # 캐시 저장을 위해 추가
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
        self.latest_file = {}
        # 캐시 파일 경로 설정
        self.cache_dir = os.path.join(data_dir, '_cache')
        if not os.path.exists(self.cache_dir):
            os.makedirs(self.cache_dir)

    def get_cache_path(self, timeframe):
        return os.path.join(self.cache_dir, f"{timeframe}_cache.pkl")

    def load_data(self, files_to_load=None):
        """
        캐시가 있다면 먼저 로드하고, 새로운 파일만 추가로 읽어 병합합니다.
        """
        new_data_loaded = False
        
        # 1. 초기 로드 시 캐시 파일 먼저 확인
        if files_to_load is None:
            for timeframe in self.timeframes:
                cache_path = self.get_cache_path(timeframe)
                if os.path.exists(cache_path):
                    try:
                        with open(cache_path, 'rb') as f:
                            cache_data = pickle.load(f)
                            self.data[timeframe] = cache_data['df']
                            self.latest_file[timeframe] = cache_data['latest_file']
                        print(f"[{timeframe}] 캐시 로드 완료 (마지막 파일: {self.latest_file[timeframe]})")
                    except Exception as e:
                        print(f"[{timeframe}] 캐시 로드 실패: {e}")
            
            # 캐시 로드 후 새로 추가된 파일이 있는지 확인
            files_to_load = self.check_for_new_data()

        # 2. 새로운 파일 처리
        for timeframe in self.timeframes:
            if timeframe not in files_to_load or not files_to_load[timeframe]:
                continue

            timeframe_data_list = []
            for file in files_to_load[timeframe]:
                try:
                    filename = os.path.basename(file)
                    # 파일명 형식: 20231027_120000_...
                    ts_part = filename.split('_')[0] + '_' + filename.split('_')[1]
                    timestamp = datetime.strptime(ts_part, '%Y%m%d_%H%M%S')
                    
                    with open(file, 'rb') as f:
                        data = orjson.loads(f.read())
                        
                    if 'result' in data and 'data' in data['result'] and 'json' in data['result']['data']:
                        snaps_data = data['result']['data']['json'].get('snaps', [])
                        formatted_ts = timestamp.strftime('%Y-%m-%d %H:%M:%S')
                        
                        for snap in snaps_data:
                            snap['timestamp'] = formatted_ts
                            timeframe_data_list.append(snap)
                        
                        self.latest_file[timeframe] = filename # 최신 파일명 갱신
                        new_data_loaded = True
                except Exception as e:
                    print(f"Error loading {file}: {e}")

            if timeframe_data_list:
                new_df = pd.DataFrame(timeframe_data_list)
                if timeframe in self.data and not self.data[timeframe].empty:
                    self.data[timeframe] = pd.concat([self.data[timeframe], new_df], ignore_index=True)
                else:
                    self.data[timeframe] = new_df
                
                # 중복 제거 및 정렬
                self.data[timeframe].drop_duplicates(subset=['username', 'timestamp'], keep='last', inplace=True)
                self.data[timeframe].sort_values('timestamp', inplace=True)

                # 3. 변경된 데이터를 캐시에 즉시 저장
                self.save_cache(timeframe)
            
        if new_data_loaded or any(not df.empty for df in self.data.values()):
            self.process_user_data()
            
        return self.data

    def save_cache(self, timeframe):
        """데이터프레임과 마지막 로드 파일명을 피클로 저장"""
        try:
            cache_path = self.get_cache_path(timeframe)
            with open(cache_path, 'wb') as f:
                pickle.dump({
                    'df': self.data[timeframe],
                    'latest_file': self.latest_file.get(timeframe, '')
                }, f)
            print(f"[{timeframe}] 캐시 저장 완료")
        except Exception as e:
            print(f"[{timeframe}] 캐시 저장 실패: {e}")

    def check_for_new_data(self):
        """기존 latest_file 이후의 파일만 탐색"""
        new_files_to_load = defaultdict(list)
        is_new_data_found = False
        
        for timeframe in self.timeframes:
            path = os.path.join(self.data_dir, timeframe)
            if not os.path.exists(path): continue
            
            all_files = sorted(glob.glob(os.path.join(path, "*.json")))
            current_latest = self.latest_file.get(timeframe, '')
            
            for file in all_files:
                filename = os.path.basename(file)
                if filename > current_latest: # 문자열 정렬로 시간 순서 비교 가능
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
        # print(self.user_data['7D'])
        if timeframe in self.user_data and username in self.user_data[timeframe]:
            history = self.user_data[timeframe][username].copy()
            # print(history)
            if history.empty:
                # print("history empty")
                return pd.DataFrame()

            # 1. timestamp를 datetime 객체로 변환 (정렬 및 샘플링을 위해 필수)
            history['timestamp'] = pd.to_datetime(history['timestamp'])
            history.sort_values('timestamp', inplace=True)

            # 2. 데이터가 많을 경우 샘플링 (최대 500개로 제한)
            # 포인트가 500개 정도면 시각적으로 충분히 상세하면서도 렌더링이 매우 빠릅니다.
            if len(history) > 500:
                # 인덱스를 활용해 균등하게 샘플링
                indices = np.linspace(0, len(history) - 1, 500).astype(int)
                history = history.iloc[indices]
            # print("history  정상")
            return history
        return pd.DataFrame()

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