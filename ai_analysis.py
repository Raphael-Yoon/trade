import google.generativeai as genai
import pandas as pd
import os
from dotenv import load_dotenv

# .env 파일에서 환경 변수 로드
env_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(env_path)

# API 키 설정 (환경 변수에서 읽어옴)
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

def analyze_stock_data(file_path):
    """
    엑셀 데이터를 읽어서 Gemini AI에게 분석을 요청합니다.
    """
    if not os.path.exists(file_path):
        return "파일을 찾을 수 없습니다."

    if not GEMINI_API_KEY:
        return "Gemini API 키가 설정되지 않았습니다. .env 파일에 GEMINI_API_KEY를 입력해주세요."

    try:
        # 엑셀 데이터 로드
        df = pd.read_excel(file_path)
        
        # 데이터 요약 (너무 많으면 API 토큰 제한에 걸릴 수 있으므로 상위 30개 정도만 요약)
        # 주요 지표 위주로 텍스트 변환
        data_summary = df.head(30).to_string(index=False)
        
        # Gemini 설정
        genai.configure(api_key=GEMINI_API_KEY)
        # 무료 등급 할당량이 가장 넉넉한 gemini-flash-latest 사용
        model = genai.GenerativeModel('gemini-flash-latest')
        
        prompt = f"""
        너는 전문 주식 퀀트 투자 분석가이자 시장 전략가야. 
        아래는 최근 수집된 국내 주식 시장의 재무 데이터(상위 30개 종목)야.
        
        [데이터]
        {data_summary}
        
        이 데이터를 바탕으로 다음을 분석하여 전문적인 투자 리포트를 작성해줘:

        1. **종합 추천 의견 (Top 5 Picks)**:
           - 제공된 **재무 데이터**와 더불어, 현재 시장의 핵심 테마(예: **반도체, AI, HBM, 밸류업 프로그램 등**)와 산업 트렌드를 종합적으로 고려하여 가장 유망한 종목 5개를 선정해줘.
           - **종목코드(숫자 6자리)는 절대 표시하지 말고**, **순위(1위~5위)**와 **종목명**만 표시해줘.
           - 각 종목이 현재 시장 흐름(산업 테마)과 어떻게 맞닿아 있는지 포함하여 추천 의견을 요약해줘.

        2. **종목별 상세 수치 데이터 분석**:
           - 선정된 5개 종목 각각에 대해, 제공된 [데이터]의 수치(PBR, ROE, 영업이익, 수급 등)를 인용하며 왜 이 종목이 정량적 관점에서 매력적인지 상세히 분석해줘.

        3. **산업 테마 및 최신 동향 분석**:
           - 선정된 5개 종목이 속한 산업군(예: 반도체, AI 관련주 등)의 최근 트렌드와 주요 이슈를 너의 지식을 바탕으로 설명하고, 해당 종목이 그 흐름에서 어떤 수혜를 입을 수 있는지 분석해줘.

        4. **시장 전망 및 투자 전략**:
           - 현재 데이터와 시장 흐름에서 보이는 특징, 그리고 투자 시 주의해야 할 리스크 요인을 전문가의 시각에서 조언해줘.

        **작성 가이드라인:**
        - 답변은 한국어로, 신뢰감 있고 전문적인 어조로 작성해줘.
        - 마크다운(Markdown) 형식을 사용하여 가독성 있게 구성해줘.
        - 리포트 전체에서 종목코드는 절대 표시하지 마.
        """
        
        response = model.generate_content(prompt)
        return response.text

    except Exception as e:
        return f"AI 분석 중 오류가 발생했습니다: {str(e)}"
