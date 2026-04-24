"""
뉴스 에이전트 v3 — Tool Use + Agentic Loop
- RSS 수집 + 본문 fetch
- Claude 분석 (Tool Use 기반 Agentic Loop)
- 시간 감쇠 적용
- 다중 기사 집계
- 스케줄러 (30분 간격)
- SQLite 로그 저장
- 오케스트레이터 연동 (run_as_agent)

v2 대비 변경점:
  - analyze_with_api() → agentic_loop() 로 교체
  - 추가된 도구 3종:
      1. search_additional_news  : 특정 키워드 추가 뉴스 검색
      2. get_dart_cross_check    : 관련 공시 교차 확인
      3. get_market_context      : 현재 주가/거래량 조회 (stub)
  - run_as_agent() : 오케스트레이터에서 함수로 호출하는 진입점

실행: python news_agent.py
      python news_agent.py --once   (1회만 실행)
      python news_agent.py --show   (저장된 시그널 조회)
"""

import argparse
import feedparser
import hashlib
import json
import math
import os
import requests
import sqlite3
import time
from bs4 import BeautifulSoup
from datetime import datetime, timezone
from pathlib import Path

try:
    from apscheduler.schedulers.blocking import BlockingScheduler
    HAS_SCHEDULER = True
except ImportError:
    HAS_SCHEDULER = False

# ─────────────────────────────────────────
# 설정
# ─────────────────────────────────────────

PORTFOLIO = {
    "tickers": ["AAPL", "MSFT", "GOOGL", "NVDA", "005930", "000660"],
    "sectors": ["Technology", "Semiconductors", "Consumer Electronics"],
}

RSS_SOURCES = {
    "한국경제":      "https://www.hankyung.com/feed/finance",
    "매일경제":      "https://www.mk.co.kr/rss/30000001/",
    "Yahoo_Finance": "https://finance.yahoo.com/rss/topstories",
    "Investing_KR":  "https://kr.investing.com/rss/news_285.rss",
}

LAMBDA = {
    "macro": 0.005, "geopolitical": 0.008, "regulation": 0.015,
    "earnings": 0.02, "product": 0.04, "personnel": 0.06,
}

MODEL          = "claude-sonnet-4-6"
MAX_TOKENS     = 2048
MAX_BODY_CHARS = 1500
MAX_PER_SOURCE = 10
TRIGGER_THRESH = 0.35
DB_PATH        = Path(__file__).parent / "signals.db"
INTERVAL_MIN   = 30

# 수집된 기사 전역 보관 (도구에서 참조)
_collected_articles: list[dict] = []


# ─────────────────────────────────────────
# Tool Use: 도구 정의
#
# Claude는 분석 중 아래 도구들을 필요하다고 판단하면 스스로 호출합니다.
# - search_additional_news : 특정 키워드로 추가 뉴스 검색
# - get_dart_cross_check   : 뉴스와 관련된 공시 존재 여부 교차 확인
# - get_market_context     : 주가 선반영 여부 판단용 시장 데이터 조회
# ─────────────────────────────────────────

NEWS_TOOLS = [
    {
        "name": "search_additional_news",
        "description": (
            "특정 키워드로 수집된 기사 중 추가 검색을 수행합니다. "
            "sentiment 판단이 불확실하거나 관련 기사가 더 필요할 때 호출하세요. "
            "예: '삼성전자 수출규제' 키워드로 관련 기사 추가 확인."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "keyword": {
                    "type": "string",
                    "description": "검색할 키워드 (예: '반도체 수출규제', 'NVDA earnings')"
                },
                "limit": {
                    "type": "integer",
                    "description": "반환할 최대 기사 수. 기본 5",
                    "default": 5
                }
            },
            "required": ["keyword"]
        }
    },
    {
        "name": "get_dart_cross_check",
        "description": (
            "특정 종목의 최근 공시 존재 여부를 확인합니다. "
            "뉴스의 신뢰도를 높이거나, 규제/실적 뉴스가 공시로 확인되는지 "
            "교차 검증할 때 호출하세요. "
            "공시가 있으면 뉴스 신뢰도(confidence)를 높일 수 있습니다."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "ticker": {
                    "type": "string",
                    "description": "종목코드 (예: 005930, AAPL)"
                }
            },
            "required": ["ticker"]
        }
    },
    {
        "name": "get_market_context",
        "description": (
            "종목의 현재 주가, 거래량, 52주 고저가 정보를 조회합니다. "
            "뉴스가 이미 주가에 선반영됐는지 판단하거나, "
            "impact 수준을 보정할 때 호출하세요."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "ticker": {
                    "type": "string",
                    "description": "종목코드 (예: 005930, AAPL)"
                }
            },
            "required": ["ticker"]
        }
    }
]


# ─────────────────────────────────────────
# Tool Use: 도구 실행 함수
# ─────────────────────────────────────────

def execute_tool(name: str, inputs: dict) -> str:
    """Claude가 요청한 도구를 실행하고 결과를 문자열로 반환"""
    if name == "search_additional_news":
        return _tool_search_additional_news(
            keyword=inputs["keyword"],
            limit=inputs.get("limit", 5)
        )
    elif name == "get_dart_cross_check":
        return _tool_get_dart_cross_check(ticker=inputs["ticker"])
    elif name == "get_market_context":
        return _tool_get_market_context(ticker=inputs["ticker"])
    else:
        return json.dumps({"error": f"알 수 없는 도구: {name}"})


def _tool_search_additional_news(keyword: str, limit: int = 5) -> str:
    """
    수집된 기사 중 키워드 매칭 검색.
    실제 운영 시 추가 RSS 호출 또는 뉴스 검색 API로 교체 가능.
    """
    results = [
        a for a in _collected_articles
        if keyword in a.get("title", "") or keyword in a.get("text", "")
    ][:limit]

    if not results:
        return json.dumps({
            "keyword": keyword,
            "count":   0,
            "articles": [],
            "note":    "수집된 기사 중 해당 키워드 없음"
        }, ensure_ascii=False)

    return json.dumps({
        "keyword":  keyword,
        "count":    len(results),
        "articles": [
            {
                "title":        a["title"][:80],
                "source":       a["source"],
                "published_at": a["published_at"],
            }
            for a in results
        ]
    }, ensure_ascii=False)


def _tool_get_dart_cross_check(ticker: str) -> str:
    """
    signals.db에서 해당 종목의 최근 공시 여부 확인.
    dart_pipeline_agentic.py가 먼저 실행됐을 때 유효합니다.
    """
    try:
        con  = sqlite3.connect(DB_PATH)
        rows = con.execute("""
            SELECT title, published_at, event_type
            FROM signals
            WHERE source_type = 'dart'
              AND title LIKE ?
            ORDER BY published_at DESC LIMIT 3
        """, (f"%{ticker}%",)).fetchall()
        con.close()

        if not rows:
            return json.dumps({
                "ticker":  ticker,
                "count":   0,
                "filings": [],
                "note":    "최근 관련 공시 없음"
            }, ensure_ascii=False)

        return json.dumps({
            "ticker":  ticker,
            "count":   len(rows),
            "filings": [
                {"title": r[0][:60], "date": r[1][:10], "event_type": r[2]}
                for r in rows
            ]
        }, ensure_ascii=False)

    except Exception as e:
        return json.dumps({"ticker": ticker, "error": str(e)})


def _tool_get_market_context(ticker: str) -> str:
    """
    주가 컨텍스트 조회 (stub).
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
        "AAPL": {
            "ticker":        "AAPL",
            "name":          "Apple Inc.",
            "current_price": 189,
            "volume_ratio":  1.1,
            "week52_high":   237,
            "week52_low":    164,
            "vs_52w_high":   "-20.3%",
            "note":          "stub 데이터 — 실제 API 연동 필요"
        },
        "NVDA": {
            "ticker":        "NVDA",
            "name":          "NVIDIA Corp.",
            "current_price": 875,
            "volume_ratio":  1.5,
            "week52_high":   974,
            "week52_low":    462,
            "vs_52w_high":   "-10.2%",
            "note":          "stub 데이터 — 실제 API 연동 필요"
        },
    }
    data = stub_data.get(ticker, {
        "ticker": ticker,
        "note":   "stub 데이터 없음 — 실제 API 연동 필요"
    })
    return json.dumps(data, ensure_ascii=False)


# ─────────────────────────────────────────
# DB 초기화
# ─────────────────────────────────────────

def init_db():
    con = sqlite3.connect(DB_PATH)
    con.execute("""
        CREATE TABLE IF NOT EXISTS signals (
            id            TEXT PRIMARY KEY,
            run_at        TEXT,
            source        TEXT,
            title         TEXT,
            url           TEXT,
            published_at  TEXT,
            relevant      INTEGER,
            event_type    TEXT,
            sentiment     REAL,
            impact        INTEGER,
            confidence    REAL,
            signal_score  REAL,
            decay_weight  REAL,
            final_score   REAL,
            trigger       INTEGER,
            urgency       TEXT,
            risk_level    TEXT,
            reason        TEXT,
            raw_json      TEXT
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS consolidated (
            run_at       TEXT PRIMARY KEY,
            signal_score REAL,
            trigger      INTEGER,
            urgency      TEXT,
            risk_level   TEXT,
            tickers      TEXT,
            source_count INTEGER,
            raw_json     TEXT
        )
    """)
    con.commit()
    con.close()


# ─────────────────────────────────────────
# 1. 뉴스 수집
# ─────────────────────────────────────────

def fetch_body(url: str) -> str:
    try:
        res  = requests.get(url, timeout=6, headers={"User-Agent": "Mozilla/5.0"})
        soup = BeautifulSoup(res.text, "html.parser")
        for tag in soup(["script", "style", "nav", "footer", "header"]):
            tag.decompose()
        text = " ".join(soup.get_text().split())
        return text[:MAX_BODY_CHARS]
    except Exception:
        return ""


def fetch_news() -> list[dict]:
    """RSS 전체 소스에서 기사 수집 + 중복 제거"""
    articles, seen = [], set()

    for source, url in RSS_SOURCES.items():
        try:
            feed = feedparser.parse(url)
        except Exception:
            continue

        for entry in feed.entries[:MAX_PER_SOURCE]:
            link = entry.get("link", "")
            uid  = hashlib.md5(link.encode()).hexdigest()
            if uid in seen:
                continue
            seen.add(uid)

            body = fetch_body(link)
            articles.append({
                "id":           uid,
                "source":       source,
                "title":        entry.get("title", ""),
                "text":         body or entry.get("summary", entry.get("title", "")),
                "url":          link,
                "published_at": entry.get("published", datetime.now(timezone.utc).isoformat()),
            })

    print(f"  수집 완료: {len(articles)}건")
    return articles


# ─────────────────────────────────────────
# 2. 시간 감쇠
# ─────────────────────────────────────────

def decay_weight(published_at: str, event_type: str) -> float:
    try:
        pub   = datetime.fromisoformat(published_at.replace("Z", "+00:00"))
        hours = (datetime.now(timezone.utc) - pub).total_seconds() / 3600
    except Exception:
        hours = 1.0
    lam = LAMBDA.get(event_type, 0.02)
    return round(math.exp(-lam * max(hours, 0)), 4)


# ─────────────────────────────────────────
# 3. 시스템 프롬프트 + 유저 프롬프트
# ─────────────────────────────────────────

SYSTEM_PROMPT = """You are a financial news analyst for a portfolio rebalancing AI system.

You have access to tools that help you gather additional context before making a final judgment.
Use tools when you are uncertain about sentiment or impact — for example:
  - Use search_additional_news when you need more articles on a specific keyword
  - Use get_dart_cross_check when a news item may be backed by a regulatory filing (increases confidence)
  - Use get_market_context when you need to know if news is already priced into the stock

After gathering enough context, output your final analysis as a single JSON object.

Rules:
- Final output must be valid JSON only. No explanation, no markdown, no code blocks.
- For each article, decide if it is relevant to the portfolio.
- sentiment: -1.0 to +1.0, impact: 1~5, confidence: 0.0~1.0
- horizon: "intraday" | "short" | "mid" | "long"
- event_type: "earnings" | "regulation" | "macro" | "geopolitical" | "product" | "personnel"
- signal_score: sentiment * (impact/5) * confidence
- tickers: only from the portfolio that are actually affected
- reason: 1~2 sentences in Korean"""


def build_user_prompt(articles: list[dict]) -> str:
    articles_block = "\n\n".join(
        f"[기사 {i+1}]\n제목: {a['title']}\n출처: {a['source']}\n"
        f"발행: {a['published_at']}\n본문: {a['text']}"
        for i, a in enumerate(articles)
    )

    return f"""Analyze the following news articles for a portfolio rebalancing system.
Use tools if needed to improve confidence, then provide the final JSON.

[PORTFOLIO]
Holdings: {", ".join(PORTFOLIO["tickers"])}
Sectors: {", ".join(PORTFOLIO["sectors"])}

[ARTICLES]
{articles_block}

[REQUIRED FINAL OUTPUT]
{{
  "relevant": true,
  "articles_analyzed": [
    {{
      "title": "...",
      "relevant": true,
      "sentiment": 0.0,
      "impact": 3,
      "confidence": 0.8,
      "horizon": "short",
      "event_type": "macro",
      "tickers": ["005930"],
      "signal_score": 0.0,
      "reason": "..."
    }}
  ],
  "consolidated_signal": {{
    "sentiment": 0.0,
    "impact": 3,
    "confidence": 0.8,
    "horizon": "short",
    "event_type": "macro",
    "tickers": [...],
    "signal_score": 0.0,
    "trigger": false,
    "urgency": "watch",
    "risk_level": "low",
    "reason": "..."
  }}
}}"""


# ─────────────────────────────────────────
# 4. Agentic Loop
#
# Claude ↔ 도구 간 반복 루프:
#   1. Claude 호출 → tool_use 블록 반환 시 도구 실행
#   2. 도구 결과를 messages에 추가 → Claude 재호출
#   3. Claude가 text만 반환할 때까지 반복
#   4. 최종 텍스트에서 JSON 파싱
# ─────────────────────────────────────────

def agentic_loop(articles: list[dict]) -> tuple[dict, dict]:
    """
    Tool Use 기반 에이전틱 루프.
    Claude가 필요하다고 판단한 도구를 스스로 호출하고,
    결과를 반영해 최종 분석 JSON을 반환합니다.
    """
    import anthropic
    client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

    messages      = [{"role": "user", "content": build_user_prompt(articles)}]
    total_input   = 0
    total_output  = 0
    tool_call_log = []
    MAX_ROUNDS    = 5

    for _ in range(MAX_ROUNDS):
        resp = client.messages.create(
            model=MODEL,
            max_tokens=MAX_TOKENS,
            system=SYSTEM_PROMPT,
            tools=NEWS_TOOLS,
            messages=messages,
        )
        total_input  += resp.usage.input_tokens
        total_output += resp.usage.output_tokens

        # (A) tool_use: Claude가 도구 호출 요청
        if resp.stop_reason == "tool_use":
            messages.append({"role": "assistant", "content": resp.content})
            tool_results = []

            for block in resp.content:
                if block.type != "tool_use":
                    continue

                print(f"    🔧 도구 호출: {block.name}({block.input})")
                result_str = execute_tool(block.name, block.input)
                tool_call_log.append({
                    "tool":   block.name,
                    "inputs": block.input,
                    "result": result_str[:200],
                })
                print(f"    ✓ 결과: {result_str[:100]}...")

                tool_results.append({
                    "type":        "tool_result",
                    "tool_use_id": block.id,
                    "content":     result_str,
                })

            messages.append({"role": "user", "content": tool_results})
            continue

        # (B) end_turn: Claude가 최종 텍스트 반환
        if resp.stop_reason == "end_turn":
            final_text = "".join(
                block.text for block in resp.content if hasattr(block, "text")
            )
            clean      = final_text.replace("```json", "").replace("```", "").strip()
            llm_output = json.loads(clean)

            cost = {
                "input_tokens":   total_input,
                "output_tokens":  total_output,
                "total_cost_usd": round(
                    (total_input / 1e6 * 3.0) + (total_output / 1e6 * 15.0), 6
                ),
                "tool_calls": len(tool_call_log),
            }

            if tool_call_log:
                print(f"    📊 도구 {len(tool_call_log)}회 호출 후 최종 판단")

            return llm_output, cost

        raise ValueError(f"예상치 못한 stop_reason: {resp.stop_reason}")

    raise RuntimeError(f"MAX_ROUNDS({MAX_ROUNDS}) 초과")


# ─────────────────────────────────────────
# 5. 결과 후처리 — 감쇠 적용 + 집계 보정
# ─────────────────────────────────────────

def postprocess(result: dict, articles: list[dict]) -> dict:
    url_map          = {a["title"]: a for a in articles}
    relevant_signals = []

    for item in result.get("articles_analyzed", []):
        if not item.get("relevant"):
            continue

        matched = url_map.get(item["title"], {})
        pub_at  = matched.get("published_at", "")
        etype   = item.get("event_type", "macro")

        dw                   = decay_weight(pub_at, etype)
        score                = item.get("signal_score", 0.0)
        item["decay_weight"] = dw
        item["final_score"]  = round(score * dw, 4)
        item["published_at"] = pub_at
        relevant_signals.append(item)

    if relevant_signals:
        total_w    = sum(s.get("impact", 1) for s in relevant_signals)
        agg_score  = sum(s["final_score"] * s.get("impact", 1)
                         for s in relevant_signals) / total_w
        agg_conf   = sum(s.get("confidence", 0) * s.get("impact", 1)
                         for s in relevant_signals) / total_w
        all_tickers = list({t for s in relevant_signals for t in s.get("tickers", [])})

        abs_score = abs(agg_score)
        urgency   = ("immediate" if abs_score > 0.6
                     else "scheduled" if abs_score > TRIGGER_THRESH
                     else "watch")
        risk      = ("high" if abs_score > 0.6
                     else "mid" if abs_score > TRIGGER_THRESH
                     else "low")

        result["consolidated_signal"] = {
            **result.get("consolidated_signal", {}),
            "signal_score": round(agg_score, 4),
            "confidence":   round(agg_conf, 4),
            "tickers":      all_tickers,
            "trigger":      abs_score > TRIGGER_THRESH,
            "urgency":      urgency,
            "risk_level":   risk,
            "source_count": len(relevant_signals),
        }

    return result


# ─────────────────────────────────────────
# 6. DB 저장
# ─────────────────────────────────────────

def save_to_db(result: dict, articles: list[dict], run_at: str):
    con     = sqlite3.connect(DB_PATH)
    url_map = {a["title"]: a for a in articles}

    for item in result.get("articles_analyzed", []):
        matched = url_map.get(item["title"], {})
        con.execute("""
            INSERT OR REPLACE INTO signals VALUES
            (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            matched.get("id", hashlib.md5(item["title"].encode()).hexdigest()),
            run_at,
            matched.get("source", ""),
            item.get("title", ""),
            matched.get("url", ""),
            item.get("published_at", ""),
            int(item.get("relevant", False)),
            item.get("event_type", ""),
            item.get("sentiment", 0.0),
            item.get("impact", 0),
            item.get("confidence", 0.0),
            item.get("signal_score", 0.0),
            item.get("decay_weight", 1.0),
            item.get("final_score", 0.0),
            int(item.get("trigger", False)),
            item.get("urgency", ""),
            item.get("risk_level", ""),
            item.get("reason", ""),
            json.dumps(item, ensure_ascii=False),
        ))

    cs = result.get("consolidated_signal", {})
    con.execute("""
        INSERT OR REPLACE INTO consolidated VALUES (?,?,?,?,?,?,?,?)
    """, (
        run_at,
        cs.get("signal_score", 0.0),
        int(cs.get("trigger", False)),
        cs.get("urgency", ""),
        cs.get("risk_level", ""),
        json.dumps(cs.get("tickers", []), ensure_ascii=False),
        cs.get("source_count", 0),
        json.dumps(cs, ensure_ascii=False),
    ))

    con.commit()
    con.close()
    print(f"  DB 저장 완료 → {DB_PATH}")


# ─────────────────────────────────────────
# 7. 오케스트레이터 연동 함수
#
# orchestrator.py의 call_news_agent()가 이 함수를 호출합니다.
# 공통 래퍼 형식으로 결과를 반환합니다.
# ─────────────────────────────────────────

def run_as_agent(tickers: list[str] = None) -> dict:
    """
    오케스트레이터에서 함수로 호출하는 진입점.

    반환 형식 (orchestrator 공통 래퍼):
    {
        "agent":  "news_agent",
        "status": "ok" | "error",
        "reason": "...",
        "data": {
            "signal_score": float,
            "confidence":   float,
            "opinion":      str,
            "trigger":      bool,
            "urgency":      str,
            "risk_level":   str,
            "event_type":   str,
            "horizon":      str,
            "affected_tickers": list,
            "source_count": int,
        }
    }
    """
    global _collected_articles

    try:
        init_db()
        articles = fetch_news()
        if not articles:
            return {"agent": "news_agent", "status": "skipped",
                    "reason": "수집된 기사 없음", "data": {}}

        _collected_articles = articles

        result, cost = agentic_loop(articles)
        result        = postprocess(result, articles)

        run_at = datetime.now(timezone.utc).isoformat()
        save_to_db(result, articles, run_at)

        cs      = result.get("consolidated_signal", {})
        score   = cs.get("signal_score", 0.0)

        # signal_score → opinion 변환 (orchestrator 공통 포맷)
        if score > 0.35:
            opinion = "BUY_BIAS"
        elif score > 0.10:
            opinion = "MILD_BUY"
        elif score < -0.35:
            opinion = "SELL_BIAS"
        elif score < -0.10:
            opinion = "MILD_SELL"
        else:
            opinion = "NEUTRAL"

        return {
            "agent":  "news_agent",
            "status": "ok",
            "reason": cs.get("reason", ""),
            "data": {
                "signal_score":     score,
                "confidence":       cs.get("confidence", 0.0),
                "opinion":          opinion,
                "trigger":          cs.get("trigger", False),
                "urgency":          cs.get("urgency", "watch"),
                "risk_level":       cs.get("risk_level", "low"),
                "event_type":       cs.get("event_type", "macro"),
                "horizon":          cs.get("horizon", "short"),
                "affected_tickers": cs.get("tickers", []),
                "source_count":     cs.get("source_count", 0),
                "tool_calls":       cost.get("tool_calls", 0),
            }
        }

    except Exception as e:
        return {"agent": "news_agent", "status": "error",
                "reason": str(e), "data": {}}


# ─────────────────────────────────────────
# 8. 메인 실행 사이클 (단독 실행용)
# ─────────────────────────────────────────

def run_cycle():
    global _collected_articles

    run_at = datetime.now(timezone.utc).isoformat()
    print(f"\n{'='*50}")
    print(f"[{run_at}] 뉴스 에이전트 실행")
    print(f"{'='*50}")

    articles = fetch_news()
    if not articles:
        print("  수집된 기사 없음. 종료.")
        return

    _collected_articles = articles

    api_key = os.environ.get("ANTHROPIC_API_KEY", "")

    if api_key:
        # API 키 있음 → Agentic Loop
        try:
            result, cost = agentic_loop(articles)
            print(f"  토큰: 입력 {cost['input_tokens']:,} / "
                  f"출력 {cost['output_tokens']:,}  "
                  f"도구 {cost['tool_calls']}회")
        except Exception as e:
            print(f"  Claude 호출 실패: {e}")
            return
    else:
        # API 키 없음 → 수동 모드 (claude.ai 붙여넣기)
        prompt = build_user_prompt(articles)
        print("\n" + "─"*50)
        print("API 키 없음 → claude.ai에 아래 프롬프트를 붙여넣으세요:")
        print("─"*50)
        print(prompt)
        print("─"*50)
        print("\n결과 JSON을 아래에 붙여넣고 Enter 두 번:")
        lines = []
        while True:
            line = input()
            if line == "" and lines and lines[-1] == "":
                break
            lines.append(line)
        raw = "\n".join(lines).replace("```json", "").replace("```", "").strip()
        try:
            result = json.loads(raw)
        except json.JSONDecodeError as e:
            print(f"  JSON 파싱 실패: {e}")
            return

    result = postprocess(result, articles)

    cs = result.get("consolidated_signal", {})
    print(f"\n  signal_score : {cs.get('signal_score', 0):+.4f}")
    print(f"  trigger      : {cs.get('trigger', False)}")
    print(f"  urgency      : {cs.get('urgency', '-')}")
    print(f"  risk_level   : {cs.get('risk_level', '-')}")
    print(f"  tickers      : {cs.get('tickers', [])}")
    print(f"  reason       : {cs.get('reason', '-')[:80]}...")

    save_to_db(result, articles, run_at)
    return result


# ─────────────────────────────────────────
# 9. 시그널 조회
# ─────────────────────────────────────────

def show_signals(n=10):
    con  = sqlite3.connect(DB_PATH)
    rows = con.execute(
        "SELECT run_at, signal_score, trigger, urgency, risk_level, tickers "
        "FROM consolidated ORDER BY run_at DESC LIMIT ?", (n,)
    ).fetchall()
    con.close()

    print(f"\n{'─'*70}")
    print(f"{'실행시각':<28} {'score':>8} {'trigger':<8} {'urgency':<12} {'risk':<6} tickers")
    print(f"{'─'*70}")
    for run_at, score, trigger, urgency, risk, tickers in rows:
        t = "YES" if trigger else "no"
        print(f"{run_at[:26]:<28} {score:>+8.4f} {t:<8} {urgency:<12} {risk:<6} {tickers}")
    print(f"{'─'*70}\n")


# ─────────────────────────────────────────
# 10. 진입점
# ─────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="뉴스 에이전트 v3 (Tool Use)")
    parser.add_argument("--once", action="store_true", help="1회만 실행")
    parser.add_argument("--show", action="store_true", help="저장된 시그널 조회")
    args = parser.parse_args()

    init_db()

    if args.show:
        show_signals()
        return

    if args.once or not HAS_SCHEDULER:
        if not HAS_SCHEDULER and not args.once:
            print("apscheduler 없음 → 1회 실행. (pip install apscheduler)")
        run_cycle()
        return

    scheduler = BlockingScheduler(timezone="Asia/Seoul")
    scheduler.add_job(run_cycle, "interval", minutes=INTERVAL_MIN, id="news_agent")
    print(f"스케줄러 시작 — {INTERVAL_MIN}분 간격 (Ctrl+C로 종료)")
    run_cycle()
    try:
        scheduler.start()
    except KeyboardInterrupt:
        print("\n종료.")


if __name__ == "__main__":
    main()