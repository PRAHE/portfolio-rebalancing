"""
뉴스 에이전트 v2
- RSS 수집 + 본문 fetch
- Claude 분석 (claude.ai 구독 사용 시 API 키 없이 프롬프트 출력)
- 시간 감쇠 적용
- 다중 기사 집계
- 스케줄러 (30분 간격)
- SQLite 로그 저장

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

MAX_BODY_CHARS  = 1500   # 본문 최대 길이
MAX_PER_SOURCE  = 10     # 소스당 최대 기사 수
TRIGGER_THRESH  = 0.35   # 리밸런싱 트리거 임계값
DB_PATH         = Path(__file__).parent / "signals.db"
INTERVAL_MIN    = 30     # 스케줄 간격 (분)

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
    """URL 본문 추출 (실패 시 빈 문자열)"""
    try:
        res = requests.get(url, timeout=6,
                           headers={"User-Agent": "Mozilla/5.0"})
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
# 3. Claude 분석 프롬프트 생성
# ─────────────────────────────────────────

def build_prompt(articles: list[dict]) -> str:
    """Claude에 넣을 프롬프트 생성 (claude.ai 또는 API 공용)"""
    articles_block = "\n\n".join(
        f"[기사 {i+1}]\n제목: {a['title']}\n출처: {a['source']}\n발행: {a['published_at']}\n본문: {a['text']}"
        for i, a in enumerate(articles)
    )

    return f"""You are a financial news analyst for a portfolio rebalancing AI system.
Analyze ALL articles below and return a single JSON object.

Rules:
- Respond in valid JSON only. No explanation, no markdown, no code blocks.
- For each article, decide if it is relevant to the portfolio.
- sentiment: -1.0 to +1.0, impact: 1~5, confidence: 0.0~1.0
- horizon: "intraday"|"short"|"mid"|"long"
- event_type: "earnings"|"regulation"|"macro"|"geopolitical"|"product"|"personnel"
- signal_score: sentiment * (impact/5) * confidence
- tickers: only from the portfolio that are actually affected
- reason: 1~2 sentences in Korean

[PORTFOLIO]
Holdings: {", ".join(PORTFOLIO["tickers"])}
Sectors: {", ".join(PORTFOLIO["sectors"])}

[ARTICLES]
{articles_block}

[REQUIRED OUTPUT]
{{
  "relevant": true,
  "articles_analyzed": [
    {{
      "title": "...",
      "relevant": true or false,
      "sentiment": 0.0,
      "impact": 3,
      "confidence": 0.8,
      "horizon": "short",
      "event_type": "macro",
      "tickers": ["AAPL"],
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
# 4. 결과 후처리 — 감쇠 적용 + 집계 보정
# ─────────────────────────────────────────

def postprocess(result: dict, articles: list[dict]) -> dict:
    """
    Claude 출력에 시간 감쇠를 적용하고
    consolidated_signal을 재계산한다.
    """
    url_map = {a["title"]: a for a in articles}
    relevant_signals = []

    for item in result.get("articles_analyzed", []):
        if not item.get("relevant"):
            continue

        # published_at 매핑
        matched = url_map.get(item["title"], {})
        pub_at  = matched.get("published_at", "")
        etype   = item.get("event_type", "macro")

        dw    = decay_weight(pub_at, etype)
        score = item.get("signal_score", 0.0)
        item["decay_weight"] = dw
        item["final_score"]  = round(score * dw, 4)
        item["published_at"] = pub_at
        relevant_signals.append(item)

    # consolidated 재계산 (impact 가중 평균)
    if relevant_signals:
        total_w    = sum(s.get("impact", 1) for s in relevant_signals)
        agg_score  = sum(s["final_score"] * s.get("impact", 1) for s in relevant_signals) / total_w
        agg_conf   = sum(s.get("confidence", 0) * s.get("impact", 1) for s in relevant_signals) / total_w
        all_tickers = list({t for s in relevant_signals for t in s.get("tickers", [])})

        abs_score  = abs(agg_score)
        urgency    = "immediate" if abs_score > 0.6 else "scheduled" if abs_score > TRIGGER_THRESH else "watch"
        risk       = "high"      if abs_score > 0.6 else "mid"       if abs_score > TRIGGER_THRESH else "low"

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
# 5. DB 저장
# ─────────────────────────────────────────

def save_to_db(result: dict, articles: list[dict], run_at: str):
    con = sqlite3.connect(DB_PATH)
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
# 6. API / claude.ai 분기
# ─────────────────────────────────────────

def analyze_with_api(prompt: str) -> dict | None:
    """ANTHROPIC_API_KEY가 있으면 API 직접 호출"""
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        return None

    import urllib.request
    body = json.dumps({
        "model": "claude-sonnet-4-20250514",
        "max_tokens": 2000,
        "messages": [{"role": "user", "content": prompt}],
    }).encode()

    req = urllib.request.Request(
        "https://api.anthropic.com/v1/messages",
        data=body,
        headers={
            "Content-Type": "application/json",
            "x-api-key": api_key,
            "anthropic-version": "2023-06-01",
        },
    )
    with urllib.request.urlopen(req, timeout=30) as res:
        data  = json.loads(res.read())
        raw   = "".join(c.get("text", "") for c in data.get("content", []))
        clean = raw.replace("```json", "").replace("```", "").strip()
        return json.loads(clean)

# ─────────────────────────────────────────
# 7. 메인 실행 사이클
# ─────────────────────────────────────────

def run_cycle():
    run_at = datetime.now(timezone.utc).isoformat()
    print(f"\n{'='*50}")
    print(f"[{run_at}] 뉴스 에이전트 실행")
    print(f"{'='*50}")

    # 수집
    articles = fetch_news()
    if not articles:
        print("  수집된 기사 없음. 종료.")
        return

    # 프롬프트 생성
    prompt = build_prompt(articles)

    # API 키 있으면 자동 분석, 없으면 프롬프트 출력
    result = analyze_with_api(prompt)

    if result is None:
        # claude.ai 수동 모드
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

    # 후처리 (감쇠 + 집계 재계산)
    result = postprocess(result, articles)

    # 출력
    cs = result.get("consolidated_signal", {})
    print(f"\n  signal_score : {cs.get('signal_score', 0):+.4f}")
    print(f"  trigger      : {cs.get('trigger', False)}")
    print(f"  urgency      : {cs.get('urgency', '-')}")
    print(f"  risk_level   : {cs.get('risk_level', '-')}")
    print(f"  tickers      : {cs.get('tickers', [])}")
    print(f"  reason       : {cs.get('reason', '-')[:80]}...")

    # 저장
    save_to_db(result, articles, run_at)
    return result

# ─────────────────────────────────────────
# 8. 시그널 조회
# ─────────────────────────────────────────

def show_signals(n=10):
    con = sqlite3.connect(DB_PATH)
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
# 9. 진입점
# ─────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="뉴스 에이전트")
    parser.add_argument("--once",  action="store_true", help="1회만 실행")
    parser.add_argument("--show",  action="store_true", help="저장된 시그널 조회")
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

    # 스케줄러 모드
    scheduler = BlockingScheduler(timezone="Asia/Seoul")
    scheduler.add_job(run_cycle, "interval", minutes=INTERVAL_MIN, id="news_agent")
    print(f"스케줄러 시작 — {INTERVAL_MIN}분 간격 (Ctrl+C로 종료)")
    run_cycle()  # 시작 즉시 1회 실행
    try:
        scheduler.start()
    except KeyboardInterrupt:
        print("\n종료.")

if __name__ == "__main__":
    main()