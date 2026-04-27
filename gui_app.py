import pandas as pd
import os

file_path = '/home/raphael/pythons/cowork/data.xlsx'
if os.path.exists(file_path):
    try:
        df = pd.read_excel(file_path)
        
        # 1. 기초 정제
        df = df[df['회계감사의견'] == '적정의견'] # 감사 리스크는 기본적으로 제외
        
        # 숫자형 변환 및 결측치 처리
        cols_to_fix = ['ROE', 'PBR', 'PER', '부채비율', '영업이익증가율(%)', '매출액증가율(%)', '영업이익률']
        for col in cols_to_fix:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            
        # 2. 다차원 스코어링 (제한 없음)
        # 점수 체계:
        # - ROE (30%)
        # - 영업이익증가율 (30%)
        # - 영업이익률 (20%)
        # - 밸류에이션 (PBR/PER 역수 활용) (20%)
        
        # 정규화 (0~1 사이로 변환)
        def normalize(series):
            if series.max() == series.min(): return series * 0
            return (series - series.min()) / (series.max() - series.min())

        df['score_roe'] = normalize(df['ROE'])
        df['score_growth'] = normalize(df['영업이익증가율(%)'])
        df['score_margin'] = normalize(df['영업이익률'])
        # PER은 낮을수록 좋으므로 역수 취함 (0 제외)
        df['score_val'] = normalize(1 / df['PER'].replace(0, 1000)) + normalize(1 / df['PBR'].replace(0, 1000))
        
        df['total_score'] = (df['score_roe'] * 0.3 + 
                             df['score_growth'] * 0.3 + 
                             df['score_margin'] * 0.2 + 
                             df['score_val'] * 0.2)
        
        # 3. 결과 추출
        top_10 = df.sort_values(by='total_score', ascending=False).head(10)
        
        print("Total Analyzed: ", len(df))
        print("\nTop 10 Comprehensive Ranking (No Limits):")
        print(top_10[['종목명', '업종', 'ROE', '영업이익증가율(%)', '영업이익률', 'PBR', 'PER', 'total_score']].to_string(index=False))
        
    except Exception as e:
        print(f"Error: {e}")
else:
    print("File not found.")
