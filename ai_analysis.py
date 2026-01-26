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
            return f"구글 AI 서비스의 일시적인 요청 제한(Rate Limit)이 발생했습니다. 약 {delay} 정도 여유를 두고 다시 시도해 주세요. (429 Resource Exhausted)"
        
        # 2. JSON 구조 내 retryDelay 패턴
        delay_match = re.search(r"['\"]retryDelay['\"]\s*:\s*['\"]([\d\.]+s|[\d\.]+ms)['\"]", err_msg)
        if delay_match:
            delay = delay_match.group(1)
            return f"구글 AI 서비스의 일시적인 요청 제한이 발생했습니다. 약 {delay} 후에 다시 시도해 주세요. (429 Resource Exhausted)"
        
        # 3. 모델 할당량 초과 메시지 패턴
        if "Quota exceeded" in err_msg:
            return f"현재 AI 모델의 분당 요청 제한(RPM)에 도달했습니다. 무료 버전의 제한으로 인해 약 1분 후 다시 시도해 주시기 바랍니다."
            
        return f"현재 AI 서비스의 일시적인 사용량 제한이 발생했습니다. 잠시 후(1~2분) 다시 시도해 주세요. ({err_msg})"
        
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
        data_summary = df.head(30).to_string(index=False)
        
        client = genai.Client(api_key=GEMINI_API_KEY)
        model_id = 'gemini-2.5-flash'  # 2.0-flash 무료 티어 비활성화로 변경
        
        prompt = f"""
        당신은 전문적인 주식 퀀트 투자 분석가이자 시장 전략 시스템입니다. 
        아래는 최근 수집된 국내 주식 시장의 재무 데이터(상위 30개 종목)입니다. 여기에는 전년 동기 대비 성장성 지표들이 포함되어 있습니다.
        
        [데이터]
        {data_summary}
        
        이 데이터를 바탕으로 다음을 분석하여 전문적인 투자 리포트를 작성해 주세요:
        
        **주의**: 
        - 데이터의 '데이터기준' 컬럼은 해당 종목의 재무 수치가 어떤 DART 보고서 기반인지 나타냅니다. 
        - **성장성 지표 활용**: '매출액증가율(%)', '영업이익증가율(%)', '순이익증가율(%)' 데이터가 있다면 이를 놓치지 말고 분석에 반영하여 기업의 실적 추이를 정확히 진단해 주세요.
        - **중요: 분기 데이터 해석 방식**: '3분기보고서', '반기보고서' 등은 당해 연도 1월부터의 **누적 실적**입니다. 3분기 매출이 작년 사업보고서 매출보다 적어 보이는 것은 당연하므로, 이를 '실적 악화'로 오해하지 말고 **9개월치 누적임을 감안하여 12개월치로 연환산(Annualizing)**하거나 기간을 명시하여 분석해 주세요.
        - **DART 공시 규칙 준수**: '2024년 사업보고서'는 '2024년 회계연도(1월~12월)' 전체 실적입니다. 보고서 연도와 실적 연도는 동일하게 취급해 주세요.
        - **중요**: 수치가 **0이거나 'NaN', 'N/A'인 지표는 분석에서 제외하고 표에도 포함하지 마세요.** 
        - **가독성 및 테이블 레이아웃**: 큰 숫자는 '조', '억' 단위를 사용하여 **'401.6조'** 또는 **'125.7조'**와 같이 간결하게 포맷팅해 주세요.

        1. **종합 추천 의견 (Top 5 Picks)**:
           - **중요**: 추천 종목 5개를 하나의 **마크다운 표(Table)** 형식으로 요약해서 먼저 보여주세요.
           - 표의 컬럼은 [순위, 종목명, 핵심 섹터, 추천 이유, 투자 포인트]로 구성해 주세요.
           - 그 다음, 각 종목별로 **### 종목명** (3단계 헤더)을 사용하여 상세 분석 내용을 작성해 주세요.
           - 각 종목별 상세 분석에서는 **추천 이유**, **핵심 섹터**, **투자 포인트**를 명확하게 구분해서 작성해 주세요.
           - 현재 시장 트렌드와 데이터상의 수치를 결합하여 왜 이 종목이 선정되었는지 논리적으로 설명해 주세요.

        2. **시장 트렌드 분석**:
           - 현재 데이터에서 보이는 주요 업종별 흐름이나 특징적인 지표 변화를 분석해 주세요.
           - 투자자들이 유의해야 할 리스크 요인이나 기회 요인을 짚어 주세요.

        3. **결론 및 투자 전략**:
           - 향후 시장 대응을 위한 구체적인 전략을 제안해 주세요.
        """

        import time
        max_retries = 3
        for attempt in range(max_retries):
            try:
                response = client.models.generate_content(
                    model=model_id,
                    contents=prompt
                )
                return response.text
            except Exception as e:
                err_msg = str(e)
                if ("429" in err_msg or "RESOURCE_EXHAUSTED" in err_msg) and attempt < max_retries - 1:
                    wait_time = (attempt + 1) * 5
                    print(f"AI 분석 제한 발생 (시도 {attempt+1}/{max_retries}). {wait_time}초 후 재시도합니다...")
                    time.sleep(wait_time)
                    continue
                raise e

    except Exception as e:
        return format_ai_error(e)

def analyze_portfolio(portfolio_data):
    """
    사용자의 포트폴리오 데이터를 분석하여 투자 의견을 생성합니다.
    """
    if not GEMINI_API_KEY:
        raise AIAnalysisError("Gemini API 키가 설정되지 않았습니다.")

    try:
        data_str = ""
        for s in portfolio_data:
            name = s.get('name', s.get('code', ''))
            data_str += f"- 종목: {name}({s.get('code', '')})\n"

            # 기본 정보
            data_str += f"  현재가: {s.get('current_price', 0):,}원, 평단가: {s.get('purchase_price', 0):,}원, 수익률: {s.get('profit_rate', 0)}%\n"

            # 투자의견 상세
            opinion = s.get('opinion', 'N/A')
            opinion_score = s.get('opinion_score', 0)
            target_price = s.get('target_price', 0)
            if opinion_score > 0:
                data_str += f"  투자의견: {opinion} (점수: {opinion_score}/5.0), 목표가: {target_price:,}원\n"
            else:
                data_str += f"  투자의견: {opinion}, 목표가: {target_price:,}원\n"

            # 밸류에이션
            per = s.get('per', 0)
            pbr = s.get('pbr', 0)
            sector_per = s.get('sector_per', 0)
            if sector_per > 0:
                data_str += f"  밸류: PER {per} (업종 {sector_per}), PBR {pbr}\n"
            else:
                data_str += f"  밸류: PER {per}, PBR {pbr}\n"

            # 성장성
            rev_growth = s.get('revenue_growth', 'N/A')
            prof_growth = s.get('profit_growth', 'N/A')
            data_str += f"  성장성: 매출 {rev_growth}%, 영업이익 {prof_growth}%\n"

            # 재무 건전성
            roe = s.get('roe', 0)
            debt_ratio = s.get('debt_ratio', 0)
            if roe > 0 or debt_ratio > 0:
                data_str += f"  재무: ROE {roe}%, 부채비율 {debt_ratio}%\n"

            # 외국인 정보
            foreign_ratio = s.get('foreign_ownership_ratio', 0)
            if foreign_ratio > 0:
                data_str += f"  외국인 보유율: {foreign_ratio}%\n"

            # 기술적 지표
            price_pos = s.get('price_position_52w', s.get('rsi_pos', 0))
            ma5_diff = s.get('ma5_diff', 0)
            ma20_diff = s.get('ma20_diff', 0)
            data_str += f"  기술적 지표: 52주 위치 {price_pos}%, MA5 이격도 {ma5_diff}%, MA20 이격도 {ma20_diff}%\n"

            # 시가총액
            market_cap_rank = s.get('market_cap_rank', '')
            if market_cap_rank and market_cap_rank != 'N/A':
                data_str += f"  시총 순위: {market_cap_rank}\n"

            # 수급 정보
            f_5d = s.get('foreign_5d_net', 0)
            i_5d = s.get('inst_5d_net', 0)
            f_20d = s.get('foreign_20d_net', 0)
            i_20d = s.get('inst_20d_net', 0)
            data_str += f"  수급: 외인(5일) {f_5d:,}, 기관(5일) {i_5d:,} / 외인(20일) {f_20d:,}, 기관(20일) {i_20d:,}\n"

            # 최신 뉴스
            news_list = s.get('news', [])
            if news_list:
                news_titles = [n.get('title', '') for n in news_list[:3]]
                data_str += f"  최신 뉴스: {', '.join(news_titles)}\n"

            data_str += "\n"

        client = genai.Client(api_key=GEMINI_API_KEY)
        model_id = 'gemini-2.5-flash'  # 2.0-flash 무료 티어 비활성화로 변경

        prompt = f"""
        당신은 전문적인 금융 데이터 분석가이자 투자 전략 시스템입니다. 
        제공된 포트폴리오 데이터를 객관적이고 논리적으로 분석하여, 각 종목별로 **[매수 / 보유 / 매도 / 비중축소]** 중 하나를 결정하고 그에 따른 전문적인 분석 리포트를 작성해 주세요.

        [보유 종목 데이터]
        {data_str}

        **작성 가이드라인**:
        1. **종합 총평**: 현재 포트폴리오의 건강 상태(수익성, 리스크, 섹터 집중도 등)를 먼저 짧게 요약해 주세요.
        2. **포트폴리오 진단 요약 표**: 각 종목의 [종목명, 투자의견, 목표가, 수익률, 핵심 전략]을 포함하는 마크다운 표를 작성해 주세요.
        3. **종목별 상세 진단 (### 종목명 형식 필수)**:
           - **결론**: 명확하게 [매수/보유/매도/비중축소] 의견 제시.
           - **상세 분석**: 평단가 대비 수익률, 전문가 목표가와의 괴리율, 실적 성장성, 최근 수급 상황을 종합적으로 분석해 주세요.
           - **기술적 분석**: 제공된 이동평균선(MA5, MA20) 이격도를 바탕으로 현재 주가가 단기/중기적으로 과열권인지, 혹은 반등 구간인지 진단해 주세요.
           - **뉴스 및 심리 분석**: 최신 뉴스 제목들을 바탕으로 현재 시장의 심리(긍정/부정/중립)를 파악하고, 이것이 향후 주가에 미칠 영향을 분석해 주세요.
           - **대응 전략**: 언제 팔아야 할지(익절가), 혹은 언제 더 사야 할지 구체적인 가이드를 제시해 주세요.
        4. **시장 대응 제언**: 현재 시장 상황에서 유의해야 할 리스크나 기회 요인을 조언해 주세요.

        **톤앤매너**:
        - 전문적이면서도 객관적인 분석가의 어조로 작성해 주세요.
        - 수치를 적극적으로 활용하여 논리적인 근거를 제시해 주세요.
        - 마크다운 형식을 사용하여 가독성 있게 작성해 주세요 (표, 불렛포인트 등 활용).
        - 답변은 한국어로 작성해 주세요.
        - **주의**: '몇 초 후에 실행하라'와 같은 비현실적인 시간 기반 조언은 배제하고, 가격대나 지표 기반의 전략을 제시해 주세요.
        """

        import time
        max_retries = 3
        for attempt in range(max_retries):
            try:
                response = client.models.generate_content(
                    model=model_id,
                    contents=prompt
                )
                return response.text
            except Exception as e:
                err_msg = str(e)
                if ("429" in err_msg or "RESOURCE_EXHAUSTED" in err_msg) and attempt < max_retries - 1:
                    wait_time = (attempt + 1) * 5
                    print(f"포트폴리오 AI 분석 제한 발생 (시도 {attempt+1}/{max_retries}). {wait_time}초 후 재시도합니다...")
                    time.sleep(wait_time)
                    continue
                raise e

    except Exception as e:
        raise AIAnalysisError(format_ai_error(e))
