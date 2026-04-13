"""
DART 공시 분석 파이프라인 — Agentic Tool Use 버전

기존 dart_pipeline_full.py 대비 변경점:
  - Claude가 분석 중 스스로 도구를 선택/호출 (Tool Use)
  - 추가된 도구 3종:
      1. get_past_filings     : 과거 공시 이력 조회
      2. get_related_filings  : 동종업계 유사 공시 조회
      3. get_market_context   : 현재가·거래량·52주 정보 조회 (stub)
  - agentic_loop() : Claude ↔ 도구 간 반복 루프 처리
  - 기존 compute_scores / validate / aggregate 로직 100% 유지

설치:
    pip install python-dotenv anthropic pandas pyarrow

실행:
    python dart_pipeline_agentic.py                  # API 모드
    python dart_pipeline_agentic.py --limit 3        # 건수 제한
    python dart_pipeline_agentic.py --after 2026-01-01
"""

import os
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

# ── 설정 (기존과 동일) ────────────────────────────────

PARQUET_DIR   = Path("dart_data")
DB_PATH       = Path("signals.db")
OUTPUT_DIR    = Path("pipeline_output")
OUTPUT_DIR.mkdir(exist_ok=True)

MODEL         = "claude-sonnet-4-6"
MAX_TOKENS    = 2048          # Tool Use 응답 여유분 확보
CONTEXT_YEARS = 1.0

PORTFOLIO = {
    "tickers": ["005930", "000660"],
    "sectors": ["Semiconductors", "Consumer Electronics"],
}

PRICE_INPUT  = 3.0
PRICE_OUTPUT = 15.0
KRW_RATE     = 1380

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

_circuit_log: dict[str, list] = {}

# 전체 공시 목록 (도구에서 참조하기 위해 전역 보관)
_all_filings: list[dict] = []


# ── Tool Use: 도구 정의 ───────────────────────────────
#
# Claude는 분석 중 아래 도구들을 필요하다고 판단하면 스스로 호출합니다.
# - get_past_filings    : 특정 종목의 과거 공시 이력 조회
# - get_related_filings : 동종업계 유사 공시 조회 (비교 컨텍스트)
# - get_market_context  : 현재 주가/거래량 정보 조회 (stub → 실제 API 연동 가능)

TOOLS = [
    {
        "name": "get_past_filings",
        "description": (
            "특정 종목(ticker)의 과거 공시 이력을 조회합니다. "
            "현재 공시와 유사한 사건이 과거에 있었는지, "
            "그때 시장 반응이 어땠는지 파악할 때 사용하세요. "
            "현재 공시의 impact나 sentiment 판단이 불확실할 때 호출하세요."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "ticker": {
                    "type": "string",
                    "description": "종목코드 (예: 005930)"
                },
                "months": {
                    "type": "integer",
                    "description": "조회 기간(개월). 기본 12",
                    "default": 12
                },
                "limit": {
                    "type": "integer",
                    "description": "최대 반환 건수. 기본 5",
                    "default": 5
                }
            },
            "required": ["ticker"]
        }
    },
    {
        "name": "get_related_filings",
        "description": (
            "동종업계 종목들의 유사한 공시를 조회합니다. "
            "특정 이벤트가 업계 전반에 영향을 미치는지, "
            "또는 해당 종목만의 이슈인지 판단할 때 사용하세요. "
            "규제·거시 이슈처럼 업계 공통 영향이 의심될 때 호출하세요."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "sector": {
                    "type": "string",
                    "description": "섹터명 (예: Semiconductors)"
                },
                "keyword": {
                    "type": "string",
                    "description": "검색 키워드 (예: 수출규제, 배당)"
                },
                "limit": {
                    "type": "integer",
                    "description": "최대 반환 건수. 기본 5",
                    "default": 5
                }
            },
            "required": ["sector", "keyword"]
        }
    },
    {
        "name": "get_market_context",
        "description": (
            "종목의 현재 주가, 거래량, 52주 고저가 등 시장 컨텍스트를 조회합니다. "
            "공시 발표 시점의 주가 수준이 이미 선반영됐는지 판단하거나, "
            "impact 수준을 보정할 때 사용하세요."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "ticker": {
                    "type": "string",
                    "description": "종목코드 (예: 005930)"
                }
            },
            "required": ["ticker"]
        }
    }
]


# ── Tool Use: 도구 실행 함수 ──────────────────────────
#
# Claude가 tool_use 블록을 반환하면 execute_tool()이 실제로 실행합니다.
# get_market_context는 stub으로, 실제 환경에서는 증권 API로 교체하세요.

def execute_tool(name: str, inputs: dict) -> str:
    """Claude가 요청한 도구를 실행하고 결과를 문자열로 반환"""

    if name == "get_past_filings":
        return _tool_get_past_filings(
            ticker=inputs["ticker"],
            months=inputs.get("months", 12),
            limit=inputs.get("limit", 5)
        )

    elif name == "get_related_filings":
        return _tool_get_related_filings(
            sector=inputs["sector"],
            keyword=inputs["keyword"],
            limit=inputs.get("limit", 5)
        )

    elif name == "get_market_context":
        return _tool_get_market_context(ticker=inputs["ticker"])

    else:
        return json.dumps({"error": f"알 수 없는 도구: {name}"})


def _tool_get_past_filings(ticker: str,
                            months: int = 12,
                            limit: int = 5) -> str:
    """전역 _all_filings에서 과거 공시 이력 조회"""
    cutoff = (datetime.now(timezone.utc) - timedelta(days=months * 30)).isoformat()

    results = [
        f for f in _all_filings
        if _ticker_in(f.get("tickers"), ticker)
        and f.get("published_at", "") >= cutoff
    ][:limit]

    if not results:
        return json.dumps({"ticker": ticker, "count": 0, "filings": []},
                          ensure_ascii=False)

    return json.dumps({
        "ticker": ticker,
        "count":  len(results),
        "filings": [
            {
                "date":  f["published_at"][:10],
                "kind":  f.get("kind", ""),
                "title": f["title"][:60],
            }
            for f in results
        ]
    }, ensure_ascii=False)


def _tool_get_related_filings(sector: str,
                               keyword: str,
                               limit: int = 5) -> str:
    """키워드로 동종업계 공시 검색"""
    results = [
        f for f in _all_filings
        if keyword in f.get("title", "") or keyword in f.get("body", "")
    ][:limit]

    if not results:
        return json.dumps({"sector": sector, "keyword": keyword,
                           "count": 0, "filings": []}, ensure_ascii=False)

    return json.dumps({
        "sector":  sector,
        "keyword": keyword,
        "count":   len(results),
        "filings": [
            {
                "date":    f["published_at"][:10],
                "ticker":  f.get("tickers", []),
                "title":   f["title"][:60],
            }
            for f in results
        ]
    }, ensure_ascii=False)


def _tool_get_market_context(ticker: str) -> str:
    """
    주가 컨텍스트 조회 (stub)
    실제 운영 시 KIS API / FinanceDataReader 등으로 교체하세요.
    """
    stub_data = {
        "005930": {
            "ticker":        "005930",
            "name":          "삼성전자",
            "current_price": 71500,
            "volume_ratio":  1.3,
            "week52_high":   88800,
            "week52_low":    56100,
            "vs_52w_high":   "-19.5%",
            "note":          "stub 데이터 — 실제 API 연동 필요"
        },
        "000660": {
            "ticker":        "000660",
            "name":          "SK하이닉스",
            "current_price": 198000,
            "volume_ratio":  0.9,
            "week52_high":   238500,
            "week52_low":    141000,
            "vs_52w_high":   "-17.0%",
            "note":          "stub 데이터 — 실제 API 연동 필요"
        },
    }
    data = stub_data.get(ticker, {
        "ticker": ticker,
        "note":   "stub 데이터 없음 — 실제 API 연동 필요"
    })
    return json.dumps(data, ensure_ascii=False)


# ── DB 초기화 (기존과 동일) ───────────────────────────

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


# ── 데이터 로드 (기존과 동일) ─────────────────────────

def _ticker_in(t, ticker: str) -> bool:
    if t is None:
        return False
    if isinstance(t, str):
        return ticker in t
    try:
        return ticker in list(t)
    except Exception:
        return False


def load_filings(kinds=None, after=None, limit=None) -> list[dict]:
    files = sorted(PARQUET_DIR.glob("dart_*.parquet"), reverse=True)
    if not files:
        print("공시 데이터 없음. dart_collector.py를 먼저 실행하세요.")
        return []

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

    df = pd.read_parquet(target_file)
    mask = df["tickers"].apply(
        lambda t: any(_ticker_in(t, tk) for tk in PORTFOLIO["tickers"])
    )
    df = df[mask]

    if kinds:
        df = df[df["kind"].isin(kinds)]
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


# ── 시스템 프롬프트 ───────────────────────────────────

SYSTEM_PROMPT = """You are a financial disclosure analyst for a Korean stock portfolio rebalancing system.

You have access to tools that help you gather additional context before making a final judgment.
Use tools when you are uncertain about impact or sentiment — for example:
  - Use get_past_filings when you need to know how similar past events affected this company
  - Use get_related_filings when the event may affect the entire sector (e.g. regulations, macro)
  - Use get_market_context when you need to know if the news is already priced in

After gathering enough context, output your final analysis as a single JSON object.

Rules:
- Final output must be valid JSON only. No explanation, no markdown, no code blocks.
- If the filing has no relevance to the portfolio, return {"relevant": false}.
- sentiment: -1.0 (very negative) to +1.0 (very positive) for the portfolio.
- impact: 1 (minor) to 5 (shock).
- confidence: 0.0 to 1.0 reflecting your certainty.
- horizon: "intraday" | "short" | "mid" | "long"
- event_type: "earnings" | "regulation" | "macro" | "geopolitical" | "product" | "personnel"
- tickers: only from the portfolio that are DIRECTLY affected.
- reason: 1~2 sentences in Korean."""


def build_user_prompt(filing: dict, portfolio: dict) -> str:
    body = filing.get("body", "") or "(본문 없음 — 제목만으로 판단)"
    return f"""Analyze the following Korean regulatory filing for a portfolio rebalancing system.

[PORTFOLIO]
Holdings: {", ".join(portfolio["tickers"])}
Sectors: {", ".join(portfolio["sectors"])}

[TODAY'S FILING]
Type: {filing.get("type", "공시")}
Published: {filing.get("published_at", "")}
Title: {filing.get("title", "")}
Body: {body}

[REQUIRED FINAL OUTPUT]
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
}}

Use tools if needed to improve confidence, then provide the final JSON."""


# ── Agentic Loop ──────────────────────────────────────
#
# 핵심 변경 부분.
# Claude ↔ 도구 간 반복 루프:
#   1. Claude 호출 → tool_use 블록 반환 시 도구 실행
#   2. 도구 결과를 messages에 추가 → Claude 재호출
#   3. Claude가 text만 반환할 때까지 반복
#   4. 최종 텍스트에서 JSON 파싱

def agentic_loop(filing: dict,
                 portfolio: dict) -> tuple[dict, dict]:
    """
    Tool Use 기반 에이전틱 루프.
    Claude가 필요하다고 판단한 도구를 스스로 호출하고,
    결과를 반영해 최종 분석 JSON을 반환합니다.
    """
    import anthropic
    client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

    messages = [
        {"role": "user", "content": build_user_prompt(filing, portfolio)}
    ]

    total_input  = 0
    total_output = 0
    tool_call_log = []

    MAX_ROUNDS = 5  # 무한 루프 방지

    for round_num in range(MAX_ROUNDS):

        resp = client.messages.create(
            model=MODEL,
            max_tokens=MAX_TOKENS,
            system=SYSTEM_PROMPT,
            tools=TOOLS,
            messages=messages,
        )

        total_input  += resp.usage.input_tokens
        total_output += resp.usage.output_tokens

        # ── stop_reason 분기 ──────────────────────────

        # (A) tool_use: Claude가 도구 호출을 요청
        if resp.stop_reason == "tool_use":
            # assistant 메시지 전체를 messages에 추가
            assistant_content = [block.__dict__ for block in resp.content]
            messages.append({"role": "assistant", "content": resp.content})

            # 모든 tool_use 블록 처리
            tool_results = []
            for block in resp.content:
                if block.type != "tool_use":
                    continue

                tool_name   = block.name
                tool_inputs = block.input
                print(f"    🔧 도구 호출: {tool_name}({tool_inputs})")

                result_str = execute_tool(tool_name, tool_inputs)
                tool_call_log.append({
                    "tool":   tool_name,
                    "inputs": tool_inputs,
                    "result": result_str[:200],  # 로그용 축약
                })
                print(f"    ✓ 결과: {result_str[:100]}...")

                tool_results.append({
                    "type":        "tool_result",
                    "tool_use_id": block.id,
                    "content":     result_str,
                })

            # 도구 결과를 user 메시지로 추가 후 재호출
            messages.append({"role": "user", "content": tool_results})
            continue

        # (B) end_turn: Claude가 최종 텍스트 반환
        if resp.stop_reason == "end_turn":
            # 텍스트 블록에서 JSON 추출
            final_text = ""
            for block in resp.content:
                if hasattr(block, "text"):
                    final_text += block.text

            clean = final_text.replace("```json", "").replace("```", "").strip()
            llm_output = json.loads(clean)

            cost = {
                "input_tokens":  total_input,
                "output_tokens": total_output,
                "total_cost_usd": round(
                    (total_input  / 1e6 * PRICE_INPUT) +
                    (total_output / 1e6 * PRICE_OUTPUT), 6
                ),
                "tool_calls": len(tool_call_log),
                "mode": "agentic",
            }

            if tool_call_log:
                print(f"    📊 도구 {len(tool_call_log)}회 호출 후 최종 판단")

            return llm_output, cost

        # (C) 예외 stop_reason
        raise ValueError(f"예상치 못한 stop_reason: {resp.stop_reason}")

    raise RuntimeError(f"MAX_ROUNDS({MAX_ROUNDS}) 초과 — 루프 종료")


# ── 검증 및 계산 (기존과 동일) ────────────────────────

def validate_schema(llm_output: dict) -> tuple[bool, str]:
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


def compute_scores(llm_output: dict, source_trust: float = 1.0) -> dict:
    s = float(llm_output["sentiment"])
    i = int(llm_output["impact"])
    c = float(llm_output["confidence"])

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


def validate_signals(output: dict,
                     portfolio_tickers: list) -> tuple[bool, str, dict]:
    if output["confidence"] < THRESHOLDS["confidence_min"]:
        output["skip_aggregation"] = True
        return True, "V-02 low confidence", output

    valid_tickers = [t for t in output.get("tickers", [])
                     if t in portfolio_tickers]
    invalid = [t for t in output.get("tickers", [])
               if t not in portfolio_tickers]
    if invalid:
        output["tickers"] = valid_tickers
        if not valid_tickers:
            return False, f"V-03 no valid tickers: {invalid}", output

    reason    = output.get("reason", "")
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
        output = compute_scores(
            {k: v for k, v in output.items()
             if k not in ("signal_score", "trigger", "urgency",
                          "risk_level", "opinion", "source_trust")},
            output.get("source_trust", 1.0)
        )

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


def compute_final_score(signal: dict) -> dict:
    try:
        pub   = datetime.fromisoformat(
            signal["published_at"].replace("Z", "+00:00"))
        hours = (datetime.now(timezone.utc) - pub).total_seconds() / 3600
    except Exception:
        hours = 1.0

    lam          = LAMBDA.get(signal.get("event_type", ""), 0.02)
    decay_weight = round(math.exp(-lam * max(hours, 0)), 4)
    final_score  = round(signal["signal_score"] * decay_weight, 4)

    return {**signal, "decay_weight": decay_weight, "final_score": final_score}


# ── 집계 (기존과 동일) ────────────────────────────────

def aggregate(signals: list[dict]) -> dict | None:
    if not signals:
        return None

    valid = [s for s in signals if not s.get("skip_aggregation")]
    if not valid:
        return None

    active = [s for s in valid
              if abs(s.get("final_score", 0)) >= THRESHOLDS["decay_min"]]
    if not active:
        return None

    total_w   = sum(s["impact"] for s in active)
    agg_score = round(sum(s["final_score"] * s["impact"]
                          for s in active) / total_w, 4)
    agg_conf  = round(sum(s["confidence"]  * s["impact"]
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
        "agent":        "dart_agent",
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


# ── 저장 (기존과 동일) ────────────────────────────────

def save_signal(signal: dict, filing: dict, run_at: str):
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

def run(kinds=None, after=None, limit=None):
    global _all_filings

    init_db()

    filings = load_filings(kinds=kinds, after=after, limit=limit)
    if not filings:
        return

    # 도구에서 참조할 수 있도록 전역 등록
    _all_filings = filings

    run_at         = datetime.now(timezone.utc).isoformat()
    total_input    = 0
    total_output   = 0
    total_tool_calls = 0
    scored_signals = []

    print(f"\n파이프라인 시작 (Agentic Tool Use) — {len(filings)}건")

    for i, filing in enumerate(filings):
        print(f"\n[{i+1}/{len(filings)}] {filing['title'][:50]}", flush=True)

        # ── Agentic Loop 호출 (핵심 변경) ──────────────
        try:
            llm_output, cost = agentic_loop(filing, PORTFOLIO)
        except Exception as e:
            print(f"  Claude 호출 실패: {e}")
            log_to_failed(filing, {}, "CLAUDE_CALL", str(e))
            continue

        total_input      += cost["input_tokens"]
        total_output     += cost["output_tokens"]
        total_tool_calls += cost.get("tool_calls", 0)

        if not llm_output.get("relevant", False):
            print(f"  relevant=false — 스킵")
            continue

        passed, reason = validate_schema(llm_output)
        if not passed:
            print(f"  {reason}")
            log_to_failed(filing, llm_output, "V-01", reason)
            continue

        source_trust = SOURCE_TRUST.get("dart", 1.0)
        scored       = compute_scores(llm_output, source_trust)

        passed, reason, scored = validate_signals(scored, PORTFOLIO["tickers"])
        if not passed:
            print(f"  {reason}")
            log_to_failed(filing, scored, reason[:3], reason)
            continue

        if scored.get("skip_aggregation"):
            print(f"  V-02 low confidence — 집계 제외")
            save_signal(scored, filing, run_at)
            continue

        scored_final = compute_final_score(scored)
        save_signal(scored_final, filing, run_at)
        scored_signals.append(scored_final)

        print(f"  signal_score={scored_final['signal_score']:+.3f} "
              f"opinion={scored_final['opinion']} "
              f"confidence={scored_final['confidence']:.2f}")

        time.sleep(0.5)

    if not scored_signals:
        print("\n유효한 시그널 없음 — 집계 생략")
        return

    consolidated = aggregate(scored_signals)
    if not consolidated:
        print("\n집계 결과 없음")
        return

    consolidated_id = save_consolidated(consolidated)

    total_cost_usd = round(
        (total_input  / 1e6 * PRICE_INPUT) +
        (total_output / 1e6 * PRICE_OUTPUT), 6
    )
    total_cost_krw = round(total_cost_usd * KRW_RATE, 1)

    print(f"\n{'='*60}")
    print(f"파이프라인 완료 (Agentic Tool Use)")
    print(f"처리: {len(filings)}건 → {len(scored_signals)}건 유효")
    print(f"도구 호출 총 {total_tool_calls}회")
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
    print(f"{'─'*60}")
    print(f"토큰: 입력 {total_input:,} / 출력 {total_output:,}")
    print(f"비용: ${total_cost_usd:.4f} (₩{total_cost_krw:.1f})")
    print(f"{'─'*60}")
    print(f"판단 에이전트 전달: trigger={consolidated['trigger']}")
    if consolidated['trigger']:
        payload = {
            "agent":            "dart_agent",
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
    parser = argparse.ArgumentParser(description="DART 공시 분석 파이프라인 (Agentic)")
    parser.add_argument("--kinds",  nargs="+", default=["B", "A"])
    parser.add_argument("--after",  default=None)
    parser.add_argument("--limit",  type=int, default=None)
    args = parser.parse_args()

    run(kinds=args.kinds, after=args.after, limit=args.limit)