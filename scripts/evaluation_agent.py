"""
평가 에이전트 (Evaluation Agent)

역할:
  - 오케스트레이터의 과거 판단 결과를 추적하고 정확도를 평가합니다.
  - 매일 장 마감 후 자동 실행됩니다.
  - 평가 결과는 오케스트레이터의 다음 판단 컨텍스트로 활용됩니다.

오케스트레이터와의 관계:
  - signals.db (decisions, evaluations 테이블) 를 공유합니다.
  - orchestrator.py와 DB 경로만 맞으면 독립적으로 실행 가능합니다.

verdict 종류:
  ACCURATE        : REBALANCE 권고 → 실제로 수익 (정확)
  FALSE_POSITIVE  : REBALANCE 권고 → 실제로 손실 (오판)
  HOLD_CORRECT    : HOLD 권고 → 유지가 맞았음
  HOLD_WRONG      : HOLD 권고 → 리밸런싱했어야 했음
  USER_CORRECT    : 사용자 거부 → 사용자 판단이 맞았음
  USER_WRONG      : 사용자 거부 → 시스템 판단이 맞았음

실행:
  python evaluation_agent.py            # 평가 실행
  python evaluation_agent.py --context  # 오케스트레이터 컨텍스트 조회
  python evaluation_agent.py --mock     # mock 피드백 주입 후 평가 (테스트용)
"""

import json
import uuid
import random
import sqlite3
import argparse
from datetime import datetime, timezone
from pathlib import Path

DB_PATH = Path("signals.db")

# 포트폴리오 종목 (오케스트레이터와 동일하게 맞춰야 함)
PORTFOLIO_TICKERS = ["005930", "000660"]


# ── DB 초기화 확인 ────────────────────────────────────
#
# evaluations 테이블이 없을 경우 생성합니다.
# decisions 테이블은 orchestrator.py가 관리합니다.

def ensure_evaluations_table():
    con = sqlite3.connect(DB_PATH)
    con.execute("""
        CREATE TABLE IF NOT EXISTS evaluations (
            id                  TEXT PRIMARY KEY,
            decision_id         TEXT,
            evaluated_at        TEXT,
            tickers             TEXT,
            price_at_decision   TEXT,
            price_3d_later      TEXT,
            price_1w_later      TEXT,
            return_3d           REAL,
            return_1w           REAL,
            direction_correct   INTEGER,
            magnitude_error     REAL,
            cost_efficiency     REAL,
            verdict             TEXT,
            note                TEXT
        )
    """)
    con.commit()
    con.close()


# ── 주가 변화 조회 ────────────────────────────────────

def get_price_change(ticker: str, days: int) -> float:
    """
    주가 변화율 조회 (mock)
    실제 운영 시 KIS API / FinanceDataReader로 교체하세요.
    반환값: 변화율 (%) — 예: +3.2, -7.1
    """
    random.seed(hash(ticker + str(days)))
    return round(random.uniform(-10.0, 10.0), 2)


# ── 단일 판단 평가 ────────────────────────────────────

def evaluate_single(decision_id: str,
                    decision: str,
                    rebalance_plan: dict,
                    signal_score: float,
                    user_feedback: str) -> dict:
    """
    단일 판단에 대한 평가를 수행합니다.

    평가 흐름:
      1. 판단 후 실제 주가 변화 수집 (3일, 1주)
      2. 방향 예측 정확도 계산
      3. signal_score 크기 대비 실제 변동 괴리 계산
      4. verdict 결정
    """
    tickers = list(rebalance_plan.keys()) if rebalance_plan else PORTFOLIO_TICKERS

    # 주가 변화 수집
    returns_3d    = {t: get_price_change(t, 3) for t in tickers}
    returns_1w    = {t: get_price_change(t, 7) for t in tickers}
    avg_return_3d = sum(returns_3d.values()) / len(returns_3d)
    avg_return_1w = sum(returns_1w.values()) / len(returns_1w)

    # 방향 예측 정확도
    # signal_score < 0 → SELL_BIAS → 실제 하락이면 correct
    # signal_score > 0 → BUY_BIAS  → 실제 상승이면 correct
    if signal_score != 0:
        direction_correct = int(
            (signal_score < 0 and avg_return_1w < 0) or
            (signal_score > 0 and avg_return_1w > 0)
        )
    else:
        direction_correct = -1  # 중립 판단은 방향 평가 제외

    # signal_score 크기 대비 실제 변동 괴리
    # (signal_score 0.5 → 기대 변동 5%, 실제 7% → 괴리 2%)
    expected_move   = abs(signal_score) * 10
    magnitude_error = round(abs(expected_move - abs(avg_return_1w)), 2)

    # verdict 결정
    if user_feedback.startswith("rejected"):
        # 사용자 거부 케이스 — 사용자와 시스템 중 누가 맞았는지
        if direction_correct == 1:
            verdict = "USER_WRONG"
            note    = "사용자가 거부했지만 시스템 판단이 정확했습니다."
        elif direction_correct == 0:
            verdict = "USER_CORRECT"
            note    = "사용자의 거부 판단이 옳았습니다."
        else:
            verdict = "NEUTRAL"
            note    = "중립 시그널로 방향 평가 불가합니다."

    elif decision == "REBALANCE":
        if direction_correct == 1:
            verdict = "ACCURATE"
            note    = "리밸런싱 권고 방향이 정확했습니다."
        else:
            verdict = "FALSE_POSITIVE"
            note    = "리밸런싱 권고가 오판이었습니다. 신뢰도 하향 검토 필요."

    elif decision == "HOLD":
        # HOLD가 맞는 경우: 실제 변화폭이 3% 미만 (리밸런싱 불필요)
        if abs(avg_return_1w) < 3.0:
            verdict = "HOLD_CORRECT"
            note    = "HOLD 판단이 적절했습니다. 변동폭이 크지 않았습니다."
        else:
            verdict = "HOLD_WRONG"
            note    = f"HOLD 판단이 아쉬웠습니다. 실제 변동 {avg_return_1w:+.1f}%."

    else:
        verdict = "BLOCKED"
        note    = "제약 조건으로 차단된 케이스입니다."

    return {
        "id":                str(uuid.uuid4()),
        "decision_id":       decision_id,
        "evaluated_at":      datetime.now(timezone.utc).isoformat(),
        "tickers":           json.dumps(tickers, ensure_ascii=False),
        "price_at_decision": json.dumps({t: 0 for t in tickers}),  # stub
        "price_3d_later":    json.dumps(returns_3d, ensure_ascii=False),
        "price_1w_later":    json.dumps(returns_1w, ensure_ascii=False),
        "return_3d":         round(avg_return_3d, 2),
        "return_1w":         round(avg_return_1w, 2),
        "direction_correct": direction_correct,
        "magnitude_error":   magnitude_error,
        "cost_efficiency":   0.0,   # 실제 거래 연동 후 계산
        "verdict":           verdict,
        "note":              note,
    }


# ── 메인 실행 ─────────────────────────────────────────

def run(mock_feedback: dict = None) -> dict:
    """
    평가 에이전트 실행.

    mock_feedback: 테스트용 사용자 반응 주입
      형식: {decision_id: "approved" | "rejected: 이유"}
      실제 운영 시 사용자 입력 또는 알림 API 응답으로 대체합니다.
    """
    ensure_evaluations_table()

    print(f"\n{'='*60}")
    print(f"[평가 에이전트] 실행")
    print(f"{'='*60}")

    con = sqlite3.connect(DB_PATH)

    # mock 피드백 주입 (실제에서는 사용자 승인/거부 응답으로 대체)
    if mock_feedback:
        for decision_id, feedback in mock_feedback.items():
            con.execute(
                "UPDATE decisions SET user_feedback = ? WHERE id = ?",
                (feedback, decision_id)
            )
        con.commit()

    # 미평가 판단 조회 (BLOCKED 제외)
    rows = con.execute("""
        SELECT id, decision, rebalance_plan, confidence,
               each_agent_signal, user_feedback
        FROM decisions
        WHERE decision != 'BLOCKED'
        ORDER BY run_at DESC
    """).fetchall()

    if not rows:
        print("  평가할 판단 없음.")
        con.close()
        return {}

    evaluated = 0
    summary   = {
        "ACCURATE": 0, "FALSE_POSITIVE": 0,
        "HOLD_CORRECT": 0, "HOLD_WRONG": 0,
        "USER_CORRECT": 0, "USER_WRONG": 0,
    }

    for row in rows:
        dec_id, decision, plan_json, confidence, signal_json, feedback = row
        plan     = json.loads(plan_json)   if plan_json   else {}
        signals  = json.loads(signal_json) if signal_json else {}
        feedback = feedback or ""

        # 에이전트들의 평균 signal_score
        scores    = [v.get("signal_score") for v in signals.values()
                     if v.get("signal_score") is not None]
        avg_score = sum(scores) / len(scores) if scores else 0.0

        result = evaluate_single(dec_id, decision, plan, avg_score, feedback)

        con.execute("""
            INSERT OR REPLACE INTO evaluations VALUES
            (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            result["id"],           result["decision_id"],
            result["evaluated_at"], result["tickers"],
            result["price_at_decision"], result["price_3d_later"],
            result["price_1w_later"],    result["return_3d"],
            result["return_1w"],    result["direction_correct"],
            result["magnitude_error"],   result["cost_efficiency"],
            result["verdict"],      result["note"],
        ))

        summary[result["verdict"]] = summary.get(result["verdict"], 0) + 1
        evaluated += 1

        fb_short = feedback[:20] or "(없음)"
        print(f"\n  [{decision}] signal_score={avg_score:+.2f}  "
              f"user='{fb_short}'")
        print(f"  → 3일 수익률: {result['return_3d']:+.1f}%  "
              f"1주 수익률: {result['return_1w']:+.1f}%")
        print(f"  → verdict: {result['verdict']}")
        print(f"  → {result['note']}")

    con.commit()
    con.close()

    total    = evaluated or 1
    accuracy = round(
        (summary.get("ACCURATE", 0) + summary.get("HOLD_CORRECT", 0))
        / total * 100, 1
    )

    print(f"\n{'─'*60}")
    print(f"[평가 완료] 총 {evaluated}건  전체 정확도: {accuracy}%")
    for k, v in summary.items():
        if v > 0:
            print(f"  {k:<20}: {v}건")
    print(f"  → 이 수치가 다음 오케스트레이터 판단 컨텍스트에 포함됩니다.")
    print(f"{'='*60}")

    return {"accuracy_pct": accuracy, "summary": summary, "evaluated": evaluated}


# ── 오케스트레이터 컨텍스트 반환 ──────────────────────

def get_context() -> dict:
    """
    오케스트레이터가 판단 전에 호출하는 함수.
    최근 평가 결과를 컨텍스트로 반환해서 LLM 프롬프트에 포함합니다.

    사용 예시 (orchestrator.py에서):
        from evaluation_agent import get_context
        ctx = get_context()
        # ctx를 오케스트레이터 시스템 프롬프트에 포함
    """
    ensure_evaluations_table()

    con = sqlite3.connect(DB_PATH)
    rows = con.execute("""
        SELECT e.verdict, e.return_1w, d.decision, d.run_at, d.confidence
        FROM evaluations e
        JOIN decisions d ON e.decision_id = d.id
        ORDER BY e.evaluated_at DESC LIMIT 10
    """).fetchall()

    acc_row = con.execute("""
        SELECT COUNT(*),
               SUM(CASE WHEN verdict IN ('ACCURATE','HOLD_CORRECT') THEN 1 ELSE 0 END)
        FROM evaluations
    """).fetchone()
    con.close()

    total, correct = acc_row if acc_row else (0, 0)
    accuracy = round(correct / total * 100, 1) if total else None

    return {
        "recent_evaluations": [
            {
                "decision":   r[2],
                "verdict":    r[0],
                "return_1w":  r[1],
                "run_at":     r[3][:10],
                "confidence": r[4],
            }
            for r in rows
        ],
        "overall_accuracy_pct": accuracy,
        "total_evaluated":      total,
    }


def show_context():
    """컨텍스트 조회 결과를 콘솔에 출력합니다."""
    ctx = get_context()
    print(f"\n{'='*60}")
    print(f"[오케스트레이터 컨텍스트]")
    print(f"{'='*60}")
    print(f"  전체 정확도: {ctx['overall_accuracy_pct']}%  "
          f"(총 {ctx['total_evaluated']}건 평가)")
    if ctx["recent_evaluations"]:
        print(f"\n  최근 평가 이력:")
        for e in ctx["recent_evaluations"]:
            print(f"    {e['run_at']}  {e['decision']:<12} "
                  f"→ {e['verdict']:<20} "
                  f"1주 수익률 {e['return_1w']:+.1f}%")
    else:
        print("  평가 이력 없음.")
    print(f"{'='*60}\n")


# ── 진입점 ────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="평가 에이전트")
    parser.add_argument("--context", action="store_true",
                        help="오케스트레이터 컨텍스트 조회")
    parser.add_argument("--mock",    action="store_true",
                        help="mock 피드백 주입 후 평가 (테스트용)")
    args = parser.parse_args()

    if args.context:
        show_context()

    elif args.mock:
        # decisions DB에서 ID를 가져와서 mock 피드백 주입
        ensure_evaluations_table()
        con = sqlite3.connect(DB_PATH)
        ids = [r[0] for r in con.execute(
            "SELECT id FROM decisions "
            "WHERE decision != 'BLOCKED' ORDER BY run_at DESC"
        ).fetchall()]
        con.close()

        mock_feedback = {}
        if len(ids) >= 1:
            mock_feedback[ids[0]] = "approved"
        if len(ids) >= 2:
            mock_feedback[ids[1]] = "approved"
        if len(ids) >= 3:
            mock_feedback[ids[2]] = "rejected: 단기 노이즈라 판단"

        run(mock_feedback)
        show_context()

    else:
        # 기본: mock 피드백 없이 평가 (user_feedback이 이미 채워진 경우)
        run()