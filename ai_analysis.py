from google import genai
import pandas as pd
import os
from dotenv import load_dotenv

# .env 파일에서 환경 변수 로드
env_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(env_path)

# API 키 설정 (환경 변수에서 읽어옴)
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

class AIAnalysisError(Exception):
    """AI 분석 중 발생하는 커스텀 에러"""
    pass

def format_ai_error(e):
    """
    AI 서비스 호출 중 발생한 오류를 사용자 친화적인 메시지로 변환합니다.
    특히 429(할당량 초과) 오류 시 남은 대기 시간을 추출합니다.
    """
    err_msg = str(e)
    
    # 429 RESOURCE_EXHAUSTED 오류 체크
    if "429" in err_msg or "RESOURCE_EXHAUSTED" in err_msg:
        import re
        
        # 1. 일반적인 텍스트 패턴: "retry after 13s" 또는 "try again in 13 seconds"
        match = re.search(r"(?:retry after|try again in) ([\d\.]+s|[\d\.]+ms|[\d\.]+초|[\d\.]+ (?:seconds|second|minutes|minute))", err_msg, re.IGNORECASE)
        if match:
            delay = match.group(1).replace('seconds', '초').replace('second', '초').replace('minutes', '분').replace('minute', '분')
            return f"현재 사용량이 초과되었습니다. {delay} 후에 다시 시도해 주세요. (429 Resource Exhausted)"
        
        # 2. JSON 구조 내 retryDelay 패턴: 'retryDelay': '13s'
        delay_match = re.search(r"['\"]retryDelay['\"]\s*:\s*['\"]([\d\.]+s|[\d\.]+ms)['\"]", err_msg)
        if delay_match:
            delay = delay_match.group(1)
            if delay.endswith('s'):
                try:
                    num_sec = float(delay[:-1])
                    delay = f"{num_sec:.1f}초"
                except:
                    delay = delay.replace('s', '초')
            elif delay.endswith('ms'):
                delay = delay.replace('ms', '밀리초')
            return f"현재 사용량이 초과되었습니다. {delay} 후에 다시 시도해 주세요. (429 Resource Exhausted)"
        
        # 3. 모델 할당량 초과 메시지 패턴
        if "Quota exceeded" in err_msg:
            return f"현재 모델의 분당 요청 제한(RPM) 또는 토큰 제한(TPM)에 도달했습니다. 약 1분 후 다시 시도해 주세요. (429 Resource Exhausted)"
            
        return f"현재 AI 서비스 할당량을 모두 사용했습니다. 잠시 후(1~2분) 다시 시도해 주세요. ({err_msg})"
        
    return f"오류가 발생했습니다: {err_msg}"

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
        
        # Gemini 설정 (새로운 google-genai SDK 사용)
        client = genai.Client(api_key=GEMINI_API_KEY)
        # 무료 등급 할당량이 가장 넉넉한 gemini-2.0-flash 사용
        model_id = 'gemini-2.0-flash'
        
        prompt = f"""
        너는 전문 주식 퀀트 투자 분석가이자 시장 전략가야. 
        아래는 최근 수집된 국내 주식 시장의 재무 데이터(상위 30개 종목)야. 여기에는 전년 동기 대비 성장성 지표들이 포함되어 있어.
        
        [데이터]
        {data_summary}
        
        이 데이터를 바탕으로 다음을 분석하여 전문적인 투자 리포트를 작성해줘:
        
        **주의**: 
        - 데이터의 '데이터기준' 컬럼은 해당 종목의 재무 수치가 어떤 DART 보고서 기반인지 나타냅니다. 
        - **성장성 지표 활용**: '매출액증가율(%)', '영업이익증가율(%)', '순이익증가율(%)' 데이터가 있다면 이를 놓치지 말고 분석에 반영하여 기업의 실적 추이를 정확히 진단해줘.
        - **중요: 분기 데이터 해석 방식**: '3분기보고서', '반기보고서' 등은 당해 연도 1월부터의 **누적 실적**입니다. 3분기 매출이 작년 사업보고서 매출보다 적어 보이는 것은 당연하므로, 이를 '실적 악화'로 오해하지 말고 **9개월치 누적임을 감안하여 12개월치로 연환산(Annualizing)**하거나 기간을 명시하여 분석해줘.
        - **DART 공시 규칙 준수**: '2024년 사업보고서'는 '2024년 회계연도(1월~12월)' 전체 실적입니다. 보고서 연도와 실적 연도는 동일하게 취급해줘.
        - **중요**: 수치가 **0이거나 'NaN', 'N/A'인 지표는 분석에서 제외하고 표에도 포함하지 마.** 
        - **가독성 및 테이블 레이아웃**: 큰 숫자는 '조', '억' 단위를 사용하여 **'401.6조'** 또는 **'125.7조'**와 같이 간결하게 포맷팅해줘.

        1. **종합 추천 의견 (Top 5 Picks)**:
           - **중요**: 각 종목의 시작은 반드시 **### 종목명** (3단계 헤더)으로 작성해줘.
           - 각 종목별로 **추천 이유**, **핵심 섹터**, **투자 포인트**를 명확하게 구분해서 작성해줘.
           - 현재 시장 트렌드와 데이터상의 수치를 결합하여 왜 이 종목이 선정되었는지 논리적으로 설명해줘.

        2. **종목별 상세 수치 데이터 분석 (표 형식 필수)**:
           - 선정된 5개 종목 각각에 대해, **데이터가 존재하는 주요 지표들만** 선별하여 **마크다운 표(Table) 형식**으로 정리해줘.
           - 성장성 지표(매출/영업이익/순이익 증가율)가 있다면 반드시 포함하여 전년 대비 실적 추이를 보여줘.
           - 특히 PBR, PER의 경우 업종평균 수치와 비교하여 해당 종목이 저평가 상태인지 분석해줘.

        3. **산업 테마 및 최신 동향 분석**:
           - 현재 시장을 주도하는 테마와 선정된 종목들의 연관성을 분석해줘.

        4. **시장 전망 및 투자 전략**:
           - 향후 대응 전략과 리스크 요인을 전문가의 시각에서 조언해줘.

        **작성 가이드라인:**
        - **친절한 디자인**: 종목명이 크게 보일 수 있도록 `###` 헤더를 절대 잊지 마.
        - **가독성**: 문단과 문단 사이, 섹션과 섹션 사이에는 빈 줄을 2개 이상 넣어 여유 있게 배치해줘.
        - **이모지**: 적절한 이모지를 사용하여 리포트가 딱딱하지 않고 친절하게 느껴지도록 해줘.
        - 답변은 한국어로, 전문적이면서도 친절한 어조로 작성해줘.
        """
        
        # 새로운 SDK 방식으로 호출
        response = client.models.generate_content(
            model=model_id,
            contents=prompt
        )
        return response.text

    except Exception as e:
        raise AIAnalysisError(format_ai_error(e))

def analyze_portfolio(portfolio_data):
    """
    수집된 포트폴리오 데이터를 바탕으로 AI에게 매수/보유/매도 의견을 요청합니다.
    """
    if not GEMINI_API_KEY:
        return "Gemini API 키가 설정되지 않았습니다."

    try:
        # 데이터 요약
        data_str = ""
        for s in portfolio_data:
            data_str += f"- 종목: {s['name']}({s['code']})\n"
            data_str += f"  현재가: {s['current_price']}원, 평단가: {s['purchase_price']}원, 수익률: {s['profit_rate']}%\n"
            data_str += f"  투자의견: {s['opinion']}, 목표가: {s['target_price']}원\n"
            data_str += f"  실적성장: 매출 {s['revenue_growth']}%, 이익 {s['profit_growth']}%\n"
            data_str += f"  수급(5일): 외인 {s['foreign_net_buy']}주, 기관 {s['inst_net_buy']}주\n"
            data_str += f"  지표: PBR {s['pbr']}, PER {s['per']}, 52주내 위치: {s['rsi_pos']}%\n\n"

        client = genai.Client(api_key=GEMINI_API_KEY)
        model_id = 'gemini-2.0-flash'

        prompt = f"""
        너는 대한민국 주식 시장의 베테랑 펀드매니저이자 투자 전략가야. 
        사용자의 포트폴리오 데이터를 분석하여 각 종목별로 **[매수 / 보유 / 매도 / 비중축소]** 중 하나를 결정하고 그 이유를 설명해줘.

        [보유 종목 데이터]
        {data_str}

        **작성 가이드라인**:
        1. **종합 총평**: 현재 포트폴리오의 건강 상태(수익성, 리스크, 섹터 집중도 등)를 먼저 짧게 요약해줘.
        2. **종목별 진단 (### 종목명 형식 필수)**:
           - **결론**: 명확하게 [매수/보유/매도/비중축소] 의견 제시.
           - **상세 분석**: 평단가 대비 수익률, 전문가 목표가와의 괴리율, 실적 성장성, 최근 수급 상황을 종합적으로 분석해줘. 
           - **대응 전략**: 언제 팔아야 할지(익절가), 혹은 언제 더 사야 할지 구체적인 가이드를 줘.
        3. **시장 대응 제언**: 현재 시장 상황에서 유의해야 할 리스크나 기회 요인을 조언해줘.

        **톤앤매너**:
        - 전문적이면서도 사용자가 이해하기 쉬운 친절한 어조로 작성해줘.
        - 수치를 적극적으로 활용하여 논리적인 근거를 제시해줘.
        - 마크다운 형식을 사용하여 가독성 있게 작성해줘 (표, 불렛포인트 등 활용).
        - 답변은 한국어로 작성해줘.
        """

        response = client.models.generate_content(
            model=model_id,
            contents=prompt
        )
        return response.text

    except Exception as e:
        raise AIAnalysisError(format_ai_error(e))
