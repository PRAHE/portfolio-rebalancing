"""
DART 공시 분석 파이프라인 (컨텍스트 포함 전체 구현)

설계 기반:
  - 버전 B: Claude는 원본값(sentiment, impact, confidence 등)만 출력
  - compute_scores()에서 signal_score, trigger, urgency, opinion 직접 계산
  - N=1년 컨텍스트 (실험 결과 기반)
  - V-01~V-05 사전 검증
  - SQLite 저장 (signals, consolidated, failed_log)

설치:
    pip install python-dotenv anthropic pandas pyarrow

실행:
    python dart_pipeline_full.py                        # API 모드
    python dart_pipeline_full.py --manual               # 수동 모드 (API 없이)
    python dart_pipeline_full.py --kinds B              # 주요사항보고만
    python dart_pipeline_full.py --after 2026-01-01     # 날짜 필터
    python dart_pipeline_full.py --limit 5              # 건수 제한
"""

import os
import re
import json
import math
import uuid
import time
import sqlite3
import argparse
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

load_dotenv()

# ── 설정 ──────────────────────────────────────────────

PARQUET_DIR = Path("dart_data")
DB_PATH     = Path("signals.db")
OUTPUT_DIR  = Path("pipeline_output")
OUTPUT_DIR.mkdir(exist_ok=True)

MODEL        = "claude-sonnet-4-6"
MAX_TOKENS   = 1000
CONTEXT_YEARS = 1.0   # 실험 결과: N=1년이 최적

PORTFOLIO = {
    "tickers": ["005930", "000660"],
    "sectors": ["Semiconductors", "Consumer Electronics"],
}

# 비용 (달러 / 1M 토큰) — Sonnet 4.6 기준
PRICE_INPUT  = 3.0
PRICE_OUTPUT = 15.0
KRW_RATE     = 1380

# 임계값 — config로 관리 (조정 가능)
THRESHOLDS = {
    "trigger":        0.35,
    "immediate":      0.60,
    "mild":           0.10,
    "confidence_min": 0.50,
    "v04_penalty":    0.70,
    "circuit_count":  3,
    "circuit_window": 3600,
    "decay_min":      0.01,
}

SOURCE_TRUST = {
    "dart":     1.0,
    "research": 1.0,
    "news_ko":  0.7,
    "news_en":  0.4,
}

LAMBDA = {
    "earnings":    0.02,
    "regulation":  0.015,
    "macro":       0.005,
    "geopolitical":0.008,
    "product":     0.04,
    "personnel":   0.06,
}

VALID_EVENT_TYPES = {"earnings", "regulation", "macro", "geopolitical", "product", "personnel"}
VALID_HORIZONS    = {"intraday", "short", "mid", "long"}

# V-05 서킷 브레이커용 인메모리 기록
_circuit_log: dict[str, list] = {}


# ── DB 초기화 ──────────────────────────────────────────

def init_db():
    con = sqlite3.connect(DB_PATH)

    con.execute("""
        CREATE TABLE IF NOT EXISTS signals (
            id              TEXT PRIMARY KEY,
            run_at          TEXT,
            source          TEXT,
            source_type     TEXT,
            title           TEXT,
            url             TEXT,
            published_at    TEXT,
            event_type      TEXT,
            sentiment       REAL,
            impact          INTEGER,
            confidence      REAL,
            signal_score    REAL,
            decay_weight    REAL,
            source_trust    REAL,
            final_score     REAL,
            trigger         INTEGER,
            urgency         TEXT,
            risk_level      TEXT,
            opinion         TEXT,
            reason          TEXT,
            skip_aggregation INTEGER DEFAULT 0
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS consolidated (
            id           TEXT PRIMARY KEY,
            run_at       TEXT,
            signal_score REAL,
            confidence   REAL,
            trigger      INTEGER,
            urgency      TEXT,
            risk_level   TEXT,
            opinion      TEXT,
            event_type   TEXT,
            horizon      TEXT,
            tickers      TEXT,
            source_count INTEGER,
            evaluated    INTEGER DEFAULT 0,
            raw_json     TEXT
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS failed_log (
            id          TEXT PRIMARY KEY,
            run_at      TEXT,
            source      TEXT,
            title       TEXT,
            fail_stage  TEXT,
            fail_reason TEXT,
            raw_json    TEXT
        )
    """)

    con.commit()
    con.close()


# ── 데이터 로드 ───────────────────────────────────────

def _ticker_in(t, ticker: str) -> bool:
    if t is None:
        return False
    if isinstance(t, str):
        return ticker in t
    try:
        return ticker in list(t)
    except Exception:
        return False


def load_filings(kinds: list = None,
                 after: str = None,
                 limit: int = None) -> list[dict]:
    """Parquet에서 공시 로드"""
    files = sorted(PARQUET_DIR.glob("dart_*.parquet"), reverse=True)
    if not files:
        print("공시 데이터 없음. dart_collector.py를 먼저 실행하세요.")
        return []

    # 포트폴리오 종목 관련 파일 찾기
    target_file = None
    for f in files:
        try:
            df_check = pd.read_parquet(f, columns=["tickers"])
            for ticker in PORTFOLIO["tickers"]:
                if df_check["tickers"].apply(lambda t: _ticker_in(t, ticker)).any():
                    target_file = f
                    break
            if target_file:
                break
        except Exception:
            continue

    if not target_file:
        print("포트폴리오 종목 관련 공시 데이터 없음")
        return []

    print(f"데이터 파일: {target_file}")
    df = pd.read_parquet(target_file)

    # 포트폴리오 종목 필터
    mask = df["tickers"].apply(
        lambda t: any(_ticker_in(t, tk) for tk in PORTFOLIO["tickers"])
    )
    df = df[mask]

    # kind 필터
    if kinds:
        df = df[df["kind"].isin(kinds)]

    # 날짜 필터
    if after:
        df = df[df["published_at"] >= after + "T00:00:00Z"]

    df = df.sort_values("published_at", ascending=False).reset_index(drop=True)

    if limit:
        df = df.head(limit)

    print(f"분석 대상: {len(df)}건")

    return [
        {
            "id":           row.get("id", str(uuid.uuid4())),
            "title":        row.get("title", ""),
            "body":         row.get("body", ""),
            "type":         row.get("type", "공시"),
            "published_at": row.get("published_at", ""),
            "kind":         row.get("kind", ""),
            "tickers":      list(row["tickers"]) if hasattr(row["tickers"], '__iter__')
                            and not isinstance(row["tickers"], str)
                            else [row["tickers"]],
            "source":       row.get("source_name", "DART"),
            "url":          row.get("url", ""),
        }
        for _, row in df.iterrows()
    ]


# ── 컨텍스트 구성 ─────────────────────────────────────

def build_context(ticker: str,
                  base_date: str,
                  all_filings: list[dict],
                  n_years: float = CONTEXT_YEARS,
                  limit: int = 10) -> str:
    """
    N=1년 이내 과거 공시 이력으로 컨텍스트 구성
    실험 결과: N=1년이 최적 (confidence 0.93~0.97)
    """
    if n_years == 0:
        return ""

    cutoff = (
        datetime.fromisoformat(base_date.replace("Z", ""))
        - timedelta(days=int(n_years * 365))
    ).strftime("%Y-%m-%dT%H:%M:%S")

    past = [
        f for f in all_filings
        if _ticker_in(f.get("tickers"), ticker)
        and f["published_at"] < base_date
        and f["published_at"] >= cutoff
    ][:limit]

    if not past:
        return ""

    rows = "\n".join([
        f"  {f['published_at'][:10]}: [{f['kind']}] {f['title'][:50]}"
        for f in past
    ])

    return f"[{ticker} 최근 {n_years}년 공시 이력 ({len(past)}건)]\n{rows}"


# ── 프롬프트 ──────────────────────────────────────────

SYSTEM_PROMPT = """You are a financial disclosure analyst for a Korean stock portfolio rebalancing system.
Your job is to read Korean regulatory filings (공시) and extract structured signals.

Rules:
- Respond in valid JSON only. No explanation, no markdown, no code blocks.
- If the filing has no relevance to the portfolio, return {"relevant": false}.
- sentiment: -1.0 (very negative) to +1.0 (very positive) for the portfolio.
- impact: 1 (minor) to 5 (shock).
- confidence: 0.0 to 1.0 reflecting your certainty.
- horizon: "intraday" | "short" | "mid" | "long"
- event_type: "earnings" | "regulation" | "macro" | "geopolitical" | "product" | "personnel"
- tickers: only from the portfolio that are DIRECTLY affected.
- reason: 1~2 sentences in Korean."""


def build_user_prompt(filing: dict,
                      portfolio: dict,
                      context: str) -> str:
    body = filing.get("body", "") or "(본문 없음 — 제목만으로 판단)"

    return f"""Analyze the following Korean regulatory filing for a portfolio rebalancing system.

[PORTFOLIO]
Holdings: {", ".join(portfolio["tickers"])}
Sectors: {", ".join(portfolio["sectors"])}

{context if context else "[컨텍스트 없음 — 초기 수집 단계]"}

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


# ── Claude 호출 ───────────────────────────────────────

def call_claude_api(prompt: str) -> tuple[dict, dict]:
    import anthropic
    client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

    resp = client.messages.create(
        model=MODEL,
        max_tokens=MAX_TOKENS,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": prompt}],
    )

    usage = resp.usage
    cost  = {
        "input_tokens":  usage.input_tokens,
        "output_tokens": usage.output_tokens,
        "total_cost_usd": round(
            (usage.input_tokens  / 1e6 * PRICE_INPUT) +
            (usage.output_tokens / 1e6 * PRICE_OUTPUT), 6
        ),
        "mode": "api",
    }

    raw    = resp.content[0].text.strip()
    clean  = raw.replace("```json", "").replace("```", "").strip()
    result = json.loads(clean)

    return result, cost


def call_claude_manual(prompt: str, filing_title: str) -> tuple[dict, dict]:
    print(f"\n{'─'*60}")
    print(f"[수동 분석] {filing_title[:50]}")
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

    in_tok  = int(len(prompt + SYSTEM_PROMPT) * 1.5)
    out_tok = int(len(raw) * 1.5)
    cost = {
        "input_tokens":  in_tok,
        "output_tokens": out_tok,
        "total_cost_usd": round(
            (in_tok  / 1e6 * PRICE_INPUT) +
            (out_tok / 1e6 * PRICE_OUTPUT), 6
        ),
        "mode": "manual (추정값)",
    }

    return result, cost


# ── V-01 스키마 검증 (Claude 출력 직후) ──────────────

def validate_schema(llm_output: dict) -> tuple[bool, str]:
    """V-01: Claude가 출력해야 하는 필드만 검증"""
    required = {"relevant", "tickers", "sectors",
                "sentiment", "impact", "confidence",
                "horizon", "event_type", "reason"}

    missing = required - llm_output.keys()
    if missing:
        return False, f"V-01 missing keys: {missing}"

    try:
        s = float(llm_output["sentiment"])
        i = int(llm_output["impact"])
        c = float(llm_output["confidence"])
    except (ValueError, TypeError) as e:
        return False, f"V-01 type error: {e}"

    if not (-1.0 <= s <= 1.0):
        return False, f"V-01 sentiment out of range: {s}"
    if not (1 <= i <= 5):
        return False, f"V-01 impact out of range: {i}"
    if not (0.0 <= c <= 1.0):
        return False, f"V-01 confidence out of range: {c}"
    if llm_output["event_type"] not in VALID_EVENT_TYPES:
        return False, f"V-01 invalid event_type: {llm_output['event_type']}"
    if llm_output["horizon"] not in VALID_HORIZONS:
        return False, f"V-01 invalid horizon: {llm_output['horizon']}"

    return True, "PASS"


# ── 값 계산 (버전 B) ──────────────────────────────────

def compute_scores(llm_output: dict,
                   source_trust: float = 1.0) -> dict:
    """
    Claude 출력 기반으로 signal_score 이하 값 직접 계산
    버전 B: Claude는 원본값만 출력, 계산은 코드에서
    """
    s  = float(llm_output["sentiment"])
    i  = int(llm_output["impact"])
    c  = float(llm_output["confidence"])

    signal_score = round(s * (i / 5) * c * source_trust, 4)
    abs_score    = abs(signal_score)

    trigger    = abs_score > THRESHOLDS["trigger"]
    urgency    = ("immediate" if abs_score > THRESHOLDS["immediate"]
                  else "scheduled" if abs_score > THRESHOLDS["trigger"]
                  else "watch")
    risk_level = ("high" if abs_score > THRESHOLDS["immediate"]
                  else "mid" if abs_score > THRESHOLDS["trigger"]
                  else "low")
    opinion    = ("BUY_BIAS"   if signal_score >  THRESHOLDS["trigger"]
                  else "MILD_BUY"  if signal_score >  THRESHOLDS["mild"]
                  else "SELL_BIAS" if signal_score < -THRESHOLDS["trigger"]
                  else "MILD_SELL" if signal_score < -THRESHOLDS["mild"]
                  else "NEUTRAL")

    return {
        **llm_output,
        "signal_score": signal_score,
        "trigger":      trigger,
        "urgency":      urgency,
        "risk_level":   risk_level,
        "opinion":      opinion,
        "source_trust": source_trust,
    }


# ── V-02~V-05 검증 ────────────────────────────────────

def validate_signals(output: dict,
                     portfolio_tickers: list) -> tuple[bool, str, dict]:
    """
    V-02: 신뢰도 하한
    V-03: 티커 교차검사
    V-04: 감성-reason 일관성
    V-05: 서킷 브레이커
    """
    # V-02 신뢰도 하한
    if output["confidence"] < THRESHOLDS["confidence_min"]:
        output["skip_aggregation"] = True
        return True, "V-02 low confidence", output

    # V-03 티커 교차검사
    valid_tickers = [t for t in output.get("tickers", [])
                     if t in portfolio_tickers]
    invalid = [t for t in output.get("tickers", [])
               if t not in portfolio_tickers]
    if invalid:
        output["tickers"] = valid_tickers
        if not valid_tickers:
            return False, f"V-03 no valid tickers: {invalid}", output

    # V-04 감성-reason 일관성
    reason   = output.get("reason", "")
    sentiment = output["sentiment"]
    POSITIVE_KW = ["성장", "호실적", "상승", "급등", "개선", "호재", "강세", "증가"]
    NEGATIVE_KW = ["하락", "급락", "우려", "악재", "손실", "위기", "약세", "감소", "제재"]

    v04_flag = False
    if sentiment < -0.3 and any(w in reason for w in POSITIVE_KW):
        v04_flag = True
    if sentiment > 0.3 and any(w in reason for w in NEGATIVE_KW):
        v04_flag = True

    if v04_flag:
        output["v04_flag"]   = True
        output["confidence"] = round(output["confidence"] * THRESHOLDS["v04_penalty"], 4)
        # V-04 페널티 후 signal_score 재계산
        output = compute_scores(
            {k: v for k, v in output.items()
             if k not in ("signal_score", "trigger", "urgency",
                          "risk_level", "opinion", "source_trust")},
            output.get("source_trust", 1.0)
        )

    # V-05 서킷 브레이커
    now = datetime.now(timezone.utc).timestamp()
    if abs(output["signal_score"]) > 0.7:
        for ticker in output.get("tickers", []):
            hits = _circuit_log.setdefault(ticker, [])
            hits = [t for t in hits if now - t < THRESHOLDS["circuit_window"]]
            hits.append(now)
            _circuit_log[ticker] = hits
            if len(hits) >= THRESHOLDS["circuit_count"]:
                return False, f"V-05 circuit breaker: {ticker} {len(hits)}건/1h", output

    return True, "PASS", output


# ── 시간 감쇠 + final_score ───────────────────────────

def compute_final_score(signal: dict) -> dict:
    """
    final_score = signal_score × decay_weight × source_trust
    decay_weight = e^(-λ·경과시간(h))
    """
    try:
        pub   = datetime.fromisoformat(
            signal["published_at"].replace("Z", "+00:00"))
        hours = (datetime.now(timezone.utc) - pub).total_seconds() / 3600
    except Exception:
        hours = 1.0

    lam          = LAMBDA.get(signal.get("event_type", ""), 0.02)
    decay_weight = round(math.exp(-lam * max(hours, 0)), 4)
    final_score  = round(signal["signal_score"] * decay_weight, 4)

    return {
        **signal,
        "decay_weight": decay_weight,
        "final_score":  final_score,
    }


# ── 집계 ──────────────────────────────────────────────

def aggregate(signals: list[dict]) -> dict | None:
    """
    impact 가중 평균으로 consolidated_signal 생성
    사후 검증용 horizon, tickers, evaluated 포함
    """
    if not signals:
        return None

    valid = [s for s in signals if not s.get("skip_aggregation")]
    if not valid:
        return None

    active = [s for s in valid
              if abs(s.get("final_score", 0)) >= THRESHOLDS["decay_min"]]
    if not active:
        return None

    total_w    = sum(s["impact"] for s in active)
    agg_score  = round(sum(s["final_score"] * s["impact"]
                           for s in active) / total_w, 4)
    agg_conf   = round(sum(s["confidence"]  * s["impact"]
                           for s in active) / total_w, 4)

    all_tickers = list({t for s in active for t in s.get("tickers", [])})

    horizon_order = {"intraday": 0, "short": 1, "mid": 2, "long": 3}
    horizon = min(
        (s["horizon"] for s in active),
        key=lambda h: horizon_order.get(h, 99)
    )

    from collections import Counter
    event_type = Counter(
        s["event_type"] for s in active
    ).most_common(1)[0][0]

    abs_score  = abs(agg_score)
    trigger    = abs_score > THRESHOLDS["trigger"]
    urgency    = ("immediate" if abs_score > THRESHOLDS["immediate"]
                  else "scheduled" if abs_score > THRESHOLDS["trigger"]
                  else "watch")
    risk_level = ("high" if abs_score > THRESHOLDS["immediate"]
                  else "mid" if abs_score > THRESHOLDS["trigger"]
                  else "low")
    opinion    = ("BUY_BIAS"   if agg_score >  THRESHOLDS["trigger"]
                  else "MILD_BUY"  if agg_score >  THRESHOLDS["mild"]
                  else "SELL_BIAS" if agg_score < -THRESHOLDS["trigger"]
                  else "MILD_SELL" if agg_score < -THRESHOLDS["mild"]
                  else "NEUTRAL")

    return {
        "run_at":       datetime.now(timezone.utc).isoformat(),
        "horizon":      horizon,
        "tickers":      all_tickers,
        "agent":        "news_agent",
        "signal_score": agg_score,
        "confidence":   agg_conf,
        "trigger":      trigger,
        "urgency":      urgency,
        "risk_level":   risk_level,
        "opinion":      opinion,
        "event_type":   event_type,
        "source_count": len(active),
        "evaluated":    0,
    }


# ── 저장 ──────────────────────────────────────────────

def save_signal(signal: dict, filing: dict, run_at: str):
    """signals 테이블에 개별 공시 분석 결과 저장"""
    con = sqlite3.connect(DB_PATH)
    con.execute("""
        INSERT OR REPLACE INTO signals VALUES
        (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        signal.get("id", str(uuid.uuid4())),
        run_at,
        filing.get("source", "DART"),
        "dart",
        filing.get("title", ""),
        filing.get("url", ""),
        filing.get("published_at", ""),
        signal.get("event_type", ""),
        signal.get("sentiment", 0),
        signal.get("impact", 0),
        signal.get("confidence", 0),
        signal.get("signal_score", 0),
        signal.get("decay_weight", 1.0),
        signal.get("source_trust", 1.0),
        signal.get("final_score", 0),
        int(signal.get("trigger", False)),
        signal.get("urgency", "watch"),
        signal.get("risk_level", "low"),
        signal.get("opinion", "NEUTRAL"),
        signal.get("reason", ""),
        int(signal.get("skip_aggregation", False)),
    ))
    con.commit()
    con.close()


def save_consolidated(consolidated: dict) -> str:
    """consolidated 테이블에 집계 시그널 저장"""
    consolidated_id = str(uuid.uuid4())
    con = sqlite3.connect(DB_PATH)
    con.execute("""
        INSERT INTO consolidated VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        consolidated_id,
        consolidated["run_at"],
        consolidated["signal_score"],
        consolidated["confidence"],
        int(consolidated["trigger"]),
        consolidated["urgency"],
        consolidated["risk_level"],
        consolidated["opinion"],
        consolidated["event_type"],
        consolidated["horizon"],
        json.dumps(consolidated["tickers"]),
        consolidated["source_count"],
        0,
        json.dumps(consolidated, ensure_ascii=False),
    ))
    con.commit()
    con.close()
    return consolidated_id


def log_to_failed(filing: dict, llm_output: dict,
                  stage: str, reason: str):
    """failed_log 테이블에 실패 로그 저장"""
    con = sqlite3.connect(DB_PATH)
    con.execute("""
        INSERT INTO failed_log VALUES (?,?,?,?,?,?,?)
    """, (
        str(uuid.uuid4()),
        datetime.now(timezone.utc).isoformat(),
        filing.get("source", "DART"),
        filing.get("title", ""),
        stage,
        reason,
        json.dumps(llm_output, ensure_ascii=False),
    ))
    con.commit()
    con.close()


# ── 전체 실행 ─────────────────────────────────────────

def run(kinds: list = None,
        after: str = None,
        limit: int = None,
        manual: bool = False):

    init_db()

    # 데이터 로드
    filings = load_filings(kinds=kinds, after=after, limit=limit)
    if not filings:
        return

    run_at = datetime.now(timezone.utc).isoformat()
    total_input  = 0
    total_output = 0
    scored_signals = []

    print(f"\n파이프라인 시작 — {len(filings)}건")
    print(f"컨텍스트 기간: {CONTEXT_YEARS}년 (실험 기반 최적값)")

    for i, filing in enumerate(filings):
        ticker = filing["tickers"][0] if filing.get("tickers") else ""
        print(f"\n[{i+1}/{len(filings)}] {filing['title'][:50]}", flush=True)

        # 컨텍스트 구성 (N=1년)
        context = build_context(
            ticker,
            filing["published_at"],
            filings,
            n_years=CONTEXT_YEARS
        )

        # 프롬프트 구성
        prompt = build_user_prompt(filing, PORTFOLIO, context)

        # Claude 호출
        try:
            if manual:
                llm_output, cost = call_claude_manual(prompt, filing["title"])
            else:
                llm_output, cost = call_claude_api(prompt)
        except Exception as e:
            print(f"  Claude 호출 실패: {e}")
            log_to_failed(filing, {}, "CLAUDE_CALL", str(e))
            continue

        total_input  += cost["input_tokens"]
        total_output += cost["output_tokens"]

        # relevant 체크
        if not llm_output.get("relevant", False):
            print(f"  relevant=false — 스킵")
            continue

        # V-01 스키마 검증
        passed, reason = validate_schema(llm_output)
        if not passed:
            print(f"  {reason}")
            log_to_failed(filing, llm_output, "V-01", reason)
            continue

        # 값 계산 (compute_scores)
        source_trust = SOURCE_TRUST.get("dart", 1.0)
        scored       = compute_scores(llm_output, source_trust)

        # V-02~V-05 검증
        passed, reason, scored = validate_signals(
            scored, PORTFOLIO["tickers"]
        )
        if not passed:
            print(f"  {reason}")
            log_to_failed(filing, scored, reason[:3], reason)
            continue

        if scored.get("skip_aggregation"):
            print(f"  V-02 low confidence — 집계 제외")
            save_signal(scored, filing, run_at)
            continue

        # 감쇠 반영
        scored_final = compute_final_score(scored)

        # 저장
        save_signal(scored_final, filing, run_at)
        scored_signals.append(scored_final)

        print(f"  signal_score={scored_final['signal_score']:+.3f} "
              f"opinion={scored_final['opinion']} "
              f"confidence={scored_final['confidence']:.2f}")

        if not manual:
            time.sleep(0.5)

    # 집계
    if not scored_signals:
        print("\n유효한 시그널 없음 — 집계 생략")
        return

    consolidated = aggregate(scored_signals)
    if not consolidated:
        print("\n집계 결과 없음")
        return

    consolidated_id = save_consolidated(consolidated)

    # 비용 집계
    total_cost_usd = round(
        (total_input  / 1e6 * PRICE_INPUT) +
        (total_output / 1e6 * PRICE_OUTPUT), 6
    )
    total_cost_krw = round(total_cost_usd * KRW_RATE, 1)

    # 최종 출력
    print(f"\n{'='*60}")
    print(f"파이프라인 완료")
    print(f"처리: {len(filings)}건 입력 → {len(scored_signals)}건 유효")
    print(f"{'─'*60}")
    print(f"[consolidated_signal]")
    print(f"  signal_score : {consolidated['signal_score']:+.4f}")
    print(f"  opinion      : {consolidated['opinion']}")
    print(f"  confidence   : {consolidated['confidence']:.2f}")
    print(f"  trigger      : {consolidated['trigger']}")
    print(f"  urgency      : {consolidated['urgency']}")
    print(f"  risk_level   : {consolidated['risk_level']}")
    print(f"  horizon      : {consolidated['horizon']}")
    print(f"  event_type   : {consolidated['event_type']}")
    print(f"  tickers      : {consolidated['tickers']}")
    print(f"  source_count : {consolidated['source_count']}")
    print(f"{'─'*60}")
    print(f"토큰: 입력 {total_input:,} / 출력 {total_output:,}")
    print(f"비용: ${total_cost_usd:.4f} (₩{total_cost_krw:.1f})")
    mode_note = "(추정값)" if manual else "(실측값)"
    print(f"      {mode_note}")
    print(f"{'─'*60}")
    print(f"판단 에이전트 전달: trigger={consolidated['trigger']}")
    if consolidated['trigger']:
        payload = {
            "agent":            "news_agent",
            "consolidated_id":  consolidated_id,
            "signal_score":     consolidated["signal_score"],
            "confidence":       consolidated["confidence"],
            "opinion":          consolidated["opinion"],
            "trigger":          consolidated["trigger"],
            "urgency":          consolidated["urgency"],
            "risk_level":       consolidated["risk_level"],
            "event_type":       consolidated["event_type"],
            "horizon":          consolidated["horizon"],
            "affected_tickers": consolidated["tickers"],
            "source_count":     consolidated["source_count"],
        }
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    print(f"{'='*60}")


# ── 진입점 ────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DART 공시 분석 파이프라인")
    parser.add_argument("--kinds",  nargs="+", default=["B", "A"],
                        help="공시 kind 필터 (기본: B A)")
    parser.add_argument("--after",  default=None,
                        help="날짜 필터 (예: 2026-01-01)")
    parser.add_argument("--limit",  type=int, default=None,
                        help="분석 건수 제한")
    parser.add_argument("--manual", action="store_true",
                        help="수동 모드 (API 키 없이 claude.ai 사용)")
    args = parser.parse_args()

    run(
        kinds=args.kinds,
        after=args.after,
        limit=args.limit,
        manual=args.manual,
    )