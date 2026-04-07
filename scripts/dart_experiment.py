"""
N년 컨텍스트 품질 비교 실험
공시 데이터가 없으면 dart_collector.py를 통해 자동 수집 후 실험 진행

설치:
    pip install python-dotenv anthropic pandas pyarrow tqdm OpenDartReader

실행:
    python dart_experiment.py --ticker 005930              # 수동 모드
    python dart_experiment.py --ticker 005930 --api        # API 모드
    python dart_experiment.py --ticker 005930 --filing-idx 1  # 2번째 최신 공시
    python dart_experiment.py --ticker 000660              # SK하이닉스
"""

import os
import sys
import json
import time
import argparse
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

load_dotenv()

# ── 설정 ──────────────────────────────────────────────

PARQUET_DIR = Path("dart_data")
PARQUET_DIR.mkdir(exist_ok=True)

OUTPUT_DIR  = Path("pipeline_output")
OUTPUT_DIR.mkdir(exist_ok=True)

# 실험할 N값 목록 (단위: 년)
N_VALUES = [0, 0.25, 0.5, 1, 2, 3]

PORTFOLIO = {
    "tickers": ["005930", "000660"],
    "sectors": ["Semiconductors", "Consumer Electronics"],
}

SYSTEM_PROMPT = """You are a financial disclosure analyst for a Korean stock portfolio rebalancing system.
Respond in valid JSON only. No explanation, no markdown.
- sentiment: -1.0 to +1.0
- impact: 1 to 5
- confidence: 0.0 to 1.0 reflecting your certainty given available context
- horizon: intraday | short | mid | long
- event_type: earnings | regulation | macro | geopolitical | product | personnel
- reason: 1~2 sentences in Korean explaining your judgment"""


# ── 데이터 확인 및 자동 수집 ──────────────────────────

def _ticker_in(t, ticker: str) -> bool:
    """
    tickers 컬럼 타입에 상관없이 ticker 포함 여부 확인
    list, numpy.ndarray → 순회
    str → 문자열 포함 여부
    """
    if t is None:
        return False
    if isinstance(t, str):
        return ticker in t
    try:
        # list, numpy.ndarray 모두 처리
        return ticker in list(t)
    except Exception:
        return False


def find_parquet(ticker: str) -> Path | None:
    """
    ticker 관련 Parquet 파일 찾기
    우선순위: dart_*_full.parquet > dart_*.parquet
    """
    for pattern in ["dart_*_full.parquet", "dart_*.parquet"]:
        files = sorted(PARQUET_DIR.glob(pattern), reverse=True)
        for f in files:
            try:
                df     = pd.read_parquet(f, columns=["tickers"])
                exists = df["tickers"].apply(lambda t: _ticker_in(t, ticker)).any()
                if exists:
                    return f
            except Exception:
                continue
    return None


def collect_for_ticker(ticker: str, start_year: int = 2015):
    """
    dart_collector.py의 수집 로직을 직접 호출
    해당 ticker 데이터만 수집
    """
    print(f"\n[자동 수집] {ticker} 공시 수집 시작 (start_year={start_year})")

    try:
        import OpenDartReader
        dart = OpenDartReader(os.environ["DART_API_KEY"])
    except KeyError:
        print("오류: .env에 DART_API_KEY가 없습니다.")
        print("DART API 키를 발급받아 .env에 추가해주세요.")
        print("발급: https://opendart.fss.or.kr")
        sys.exit(1)
    except ImportError:
        print("오류: pip install OpenDartReader 를 먼저 실행하세요.")
        sys.exit(1)

    import re
    import hashlib

    KIND_LABEL = {
        'A': '공시-정기',
        'B': '공시-주요사항',
        'C': '공시-발행',
        'D': '공시-지분',
    }

    def extract_text(raw: str, max_chars: int = 500) -> str:
        text = re.sub(r'<[^>]+>', ' ', raw)
        text = re.sub(r'\s+', ' ', text).strip()
        return text[:max_chars] if len(text) >= 10 else ""

    def parse_date(rcept_dt: str) -> str:
        try:
            return datetime.strptime(str(rcept_dt), "%Y%m%d").strftime("%Y-%m-%dT00:00:00Z")
        except Exception:
            return ""

    start = f"{start_year}0101"
    end   = datetime.now().strftime("%Y%m%d")
    rows  = []

    for kind in ["B", "A", "C", "D"]:
        print(f"  [{ticker}] kind={kind} 수집 중...", flush=True)
        try:
            df = dart.list(ticker, start=start, end=end, kind=kind, final=True)
        except Exception as e:
            print(f"  [{ticker}] kind={kind} 실패: {e}")
            continue

        if df is None or df.empty:
            print(f"  [{ticker}] kind={kind} 결과 없음")
            continue

        print(f"  [{ticker}] kind={kind} {len(df)}건 발견")

        for idx, (_, row) in enumerate(df.iterrows()):
            rcept_no = row.get("rcept_no", "")
            title    = row.get("report_nm", "")
            print(f"    {idx+1}/{len(df)} {title[:40]}", flush=True)

            body = ""
            try:
                doc  = dart.document(rcept_no)
                body = extract_text(doc)
            except Exception as e:
                print(f"    본문 추출 실패: {e}")

            doc_id = hashlib.md5(rcept_no.encode()).hexdigest()
            rows.append({
                "id":           doc_id,
                "rcept_no":     rcept_no,
                "source_type":  "dart",
                "source_name":  "DART",
                "tickers":      [ticker],
                "trust_score":  1.0,
                "kind":         kind,
                "title":        title,
                "body":         body,
                "type":         KIND_LABEL.get(kind, "공시"),
                "published_at": parse_date(str(row.get("rcept_dt", ""))),
                "url":          f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={rcept_no}",
                "collected_at": datetime.now().isoformat(),
            })
            time.sleep(0.3)

        time.sleep(1)

    if not rows:
        print(f"[자동 수집] {ticker} 수집된 데이터 없음")
        sys.exit(1)

    result_df  = pd.DataFrame(rows).drop_duplicates(subset=["id"])
    ts         = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path   = PARQUET_DIR / f"dart_{start_year}_{ticker}_{ts}.parquet"
    result_df.to_parquet(out_path, index=False, compression="gzip")
    print(f"[자동 수집] 완료: {out_path} ({len(result_df)}건)")

    return out_path


def ensure_data(ticker: str, start_year: int = 2015) -> Path:
    """
    데이터가 있으면 경로 반환
    없으면 자동 수집 후 경로 반환
    """
    path = find_parquet(ticker)

    if path:
        print(f"기존 데이터 발견: {path}")
        return path

    print(f"{ticker} 공시 데이터가 없습니다. 자동 수집을 시작합니다.")
    confirm = input("수집을 시작하시겠어요? (y/n): ").strip().lower()
    if confirm != "y":
        print("취소되었습니다.")
        sys.exit(0)

    return collect_for_ticker(ticker, start_year)


# ── 데이터 로드 ───────────────────────────────────────

def load_filings(parquet_path: Path, ticker: str) -> pd.DataFrame:
    df = pd.read_parquet(parquet_path)
    df = df[df["tickers"].apply(lambda t: _ticker_in(t, ticker))]
    df = df[df["kind"].isin(["B", "A"])]
    df = df.sort_values("published_at", ascending=False).reset_index(drop=True)
    return df


# ── 컨텍스트 구성 ─────────────────────────────────────

def get_context(df: pd.DataFrame,
                base_date: str,
                n_years: float,
                limit: int = 10) -> str:
    if n_years == 0:
        return ""

    cutoff = (
        datetime.fromisoformat(base_date.replace("Z", ""))
        - timedelta(days=int(n_years * 365))
    ).isoformat()

    past = df[
        (df["published_at"] < base_date) &
        (df["published_at"] >= cutoff + "Z")
    ].head(limit)

    if past.empty:
        return ""

    rows = "\n".join([
        f"  {r['published_at'][:10]}: [{r['kind']}] {r['title'][:50]}"
        for _, r in past.iterrows()
    ])

    return f"[최근 {n_years}년 공시 이력 ({len(past)}건)]\n{rows}"


# ── 프롬프트 구성 ─────────────────────────────────────

def build_prompt(filing: dict, context: str, portfolio: dict) -> str:
    body = filing.get("body", "") or "(본문 없음 — 제목만으로 판단)"

    return f"""Analyze the following Korean regulatory filing for a portfolio rebalancing system.

[PORTFOLIO]
Holdings: {", ".join(portfolio["tickers"])}
Sectors: {", ".join(portfolio["sectors"])}

{context if context else "[컨텍스트 없음 — 오늘 공시만으로 판단]"}

[TODAY'S FILING]
Type: {filing.get("type", "공시")}
Published: {filing.get("published_at", "")}
Title: {filing.get("title", "")}
Body: {body}

[REQUIRED OUTPUT]
{{
  "relevant": true,
  "tickers": [...],
  "sectors": [...],
  "sentiment": <float -1.0 to 1.0>,
  "impact": <int 1 to 5>,
  "confidence": <float 0.0 to 1.0>,
  "horizon": "<intraday|short|mid|long>",
  "event_type": "<type>",
  "reason": "<Korean 1~2 sentences>"
}}"""


# ── Claude 호출 (API) ─────────────────────────────────

def call_claude_api(prompt: str) -> tuple[dict, dict]:
    import anthropic
    client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

    resp = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=1000,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": prompt}],
    )

    usage = resp.usage
    cost  = {
        "input_tokens":  usage.input_tokens,
        "output_tokens": usage.output_tokens,
        "total_cost_usd": round(
            (usage.input_tokens  / 1e6 * 3.0) +
            (usage.output_tokens / 1e6 * 15.0), 6
        ),
        "note": "실측값",
    }

    raw    = resp.content[0].text.strip()
    clean  = raw.replace("```json", "").replace("```", "").strip()
    result = json.loads(clean)

    return result, cost


# ── Claude 호출 (수동) ────────────────────────────────

def call_claude_manual(prompt: str, n: float) -> tuple[dict, dict]:
    print(f"\n{'─'*60}")
    print(f"[N={n}년] 아래를 claude.ai에 붙여넣으세요.")
    print(f"{'─'*60}")
    print(f"[System Prompt]\n{SYSTEM_PROMPT}")
    print(f"\n[User Prompt]\n{prompt}")
    print(f"{'─'*60}")

    print("\nClaude 응답 JSON 붙여넣기 (빈 줄 두 번으로 완료):")
    lines = []
    while True:
        line = input()
        if line == "" and lines and lines[-1] == "":
            break
        lines.append(line)

    raw    = "\n".join(lines).replace("```json", "").replace("```", "").strip()
    result = json.loads(raw)

    # 토큰 추정 (글자 수 기반)
    in_tok  = int(len(prompt + SYSTEM_PROMPT) * 1.5)
    out_tok = int(len(raw) * 1.5)
    cost = {
        "input_tokens":  in_tok,
        "output_tokens": out_tok,
        "total_cost_usd": round(
            (in_tok  / 1e6 * 3.0) +
            (out_tok / 1e6 * 15.0), 6
        ),
        "note": "추정값 (글자 수 기반)",
    }

    return result, cost


# ── 실험 실행 ─────────────────────────────────────────

def run_experiment(ticker: str,
                   filing_idx: int = 0,
                   use_api: bool = False,
                   start_year: int = 2015):

    # 데이터 확인 및 자동 수집
    parquet_path = ensure_data(ticker, start_year)

    # 공시 데이터 로드
    df = load_filings(parquet_path, ticker)

    if df.empty:
        print(f"{ticker} 분석 가능한 공시(B, A)가 없습니다.")
        sys.exit(1)

    print(f"\n총 {len(df)}건 공시 로드 완료 (B, A 종류만)")

    # 분석 대상 공시 선택
    if filing_idx >= len(df):
        print(f"filing_idx={filing_idx}가 범위를 벗어납니다. (최대: {len(df)-1})")
        sys.exit(1)

    target = df.iloc[filing_idx].to_dict()
    print(f"\n분석 대상 공시:")
    print(f"  제목: {target['title']}")
    print(f"  날짜: {target['published_at'][:10]}")
    print(f"  종류: {target.get('kind', '')}")
    print(f"\nN값별 실험 시작: {N_VALUES}")

    results = []

    for n in N_VALUES:
        print(f"\n{'─'*40}")
        print(f"[N={n}년] 실험 중...", flush=True)

        context     = get_context(df, target["published_at"], n)
        prompt      = build_prompt(target, context, PORTFOLIO)
        ctx_lines   = len(context.split("\n")) if context else 0

        print(f"  컨텍스트: {ctx_lines}줄 / 프롬프트 길이: {len(prompt)}자")

        try:
            if use_api:
                result, cost = call_claude_api(prompt)
            else:
                result, cost = call_claude_manual(prompt, n)
        except json.JSONDecodeError as e:
            print(f"  JSON 파싱 실패: {e} — 이 N값 건너뜀")
            continue
        except Exception as e:
            print(f"  오류: {e} — 이 N값 건너뜀")
            continue

        results.append({
            "n_years":       n,
            "context_lines": ctx_lines,
            "prompt_len":    len(prompt),
            "sentiment":     result.get("sentiment", 0),
            "impact":        result.get("impact", 0),
            "confidence":    result.get("confidence", 0),
            "horizon":       result.get("horizon", ""),
            "event_type":    result.get("event_type", ""),
            "reason":        result.get("reason", ""),
            "input_tokens":  cost.get("input_tokens", 0),
            "output_tokens": cost.get("output_tokens", 0),
            "cost_usd":      cost.get("total_cost_usd", 0),
            "cost_krw":      round(cost.get("total_cost_usd", 0) * 1380, 1),
            "cost_note":     cost.get("note", ""),
        })

    if not results:
        print("결과 없음 — 실험 종료")
        return

    # ── 결과 출력 ──────────────────────────────────────
    print(f"\n{'='*70}")
    print(f"실험 결과 — {ticker} / {target['title'][:40]}")
    print(f"{'='*70}")
    print(f"{'N(년)':<8} {'confidence':<12} {'sentiment':<12} {'impact':<8} {'비용(₩)':<10} reason")
    print(f"{'─'*70}")
    for r in results:
        print(
            f"{str(r['n_years']):<8} "
            f"{r['confidence']:<12.2f} "
            f"{r['sentiment']:<+12.2f} "
            f"{r['impact']:<8} "
            f"{r['cost_krw']:<10.1f} "
            f"{r['reason'][:35]}"
        )
    print(f"{'='*70}")

    # ── 인사이트 ──────────────────────────────────────
    if len(results) >= 2:
        conf_values = [r["confidence"] for r in results]
        max_conf    = max(conf_values)
        max_n       = results[conf_values.index(max_conf)]["n_years"]

        plateau = None
        for i in range(1, len(results)):
            diff = results[i]["confidence"] - results[i-1]["confidence"]
            if diff < 0.02:
                plateau = results[i]["n_years"]
                break

        cost_n0  = results[0]["cost_krw"]
        cost_max = results[-1]["cost_krw"]

        print(f"\n[인사이트]")
        print(f"  최고 confidence: N={max_n}년 ({max_conf:.2f})")
        if plateau:
            print(f"  품질 향상 둔화 시점: N={plateau}년 이후")
            print(f"  → 권장 컨텍스트 기간: {plateau}년")
        else:
            print(f"  → 데이터가 많을수록 지속적으로 품질 향상")
        print(f"  비용 변화: N=0 ₩{cost_n0:.1f} → N={N_VALUES[-1]} ₩{cost_max:.1f} "
              f"(+{cost_max-cost_n0:.1f}₩)")
        print(f"  ({cost.get('note', '')})")

    # ── 저장 ──────────────────────────────────────────
    ts   = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = OUTPUT_DIR / f"experiment_{ticker}_{ts}.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump({
            "ticker":      ticker,
            "filing":      target["title"],
            "filing_date": target["published_at"][:10],
            "n_values":    N_VALUES,
            "results":     results,
        }, f, ensure_ascii=False, indent=2)

    print(f"\n결과 저장: {path}")


# ── 진입점 ────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="N년 컨텍스트 품질 비교 실험")
    parser.add_argument("--ticker",      default="005930",
                        help="종목코드 (기본: 005930 삼성전자)")
    parser.add_argument("--filing-idx",  type=int, default=0,
                        help="분석할 공시 순서 — 0=최신 (기본: 0)")
    parser.add_argument("--api",         action="store_true",
                        help="API 모드 사용 (기본: 수동 모드)")
    parser.add_argument("--start-year",  type=int, default=2015,
                        help="자동 수집 시작 연도 (기본: 2015)")
    args = parser.parse_args()

    run_experiment(
        ticker=args.ticker,
        filing_idx=args.filing_idx,
        use_api=args.api,
        start_year=args.start_year,
    )