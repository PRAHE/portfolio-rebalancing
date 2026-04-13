"""
포트폴리오 리밸런싱 오케스트레이터

구조:
  - orchestrator.py
      ├── call_dart_agent()    → stub (dart_pipeline_agentic.py 연동 예정)
      ├── call_news_agent()    → stub
      ├── call_report_agent()  → stub
      ├── call_profit_agent()  → stub
      └── call_cost_agent()    → stub

  - 공통 래퍼 형식:
      {
        "agent":  "dart_agent",
        "status": "ok" | "error" | "skipped",
        "reason": "...",
        "data":   { ... 에이전트별 고유 데이터 }
      }

실행:
    python orchestrator.py                  # mock 모드 (API 키 불필요)
    python orchestrator.py --real           # 실제 Claude API 호출
    python orchestrator.py --scenario bear  # 시나리오 선택 (bull/bear/neutral)
"""

import os
import json
import argparse
from datetime import datetime, timezone

from dotenv import load_dotenv
load_dotenv()

# ── 포트폴리오 상태 (입력값) ──────────────────────────

PORTFOLIO_STATE = {
    "tickers": ["005930", "000660"],
    "weights": {"005930": 0.60, "000660": 0.40},   # 현재 비중
    "total_value_krw": 10_000_000,                  # 총 평가금액
    "last_rebalanced": "2026-03-01",
}


# ── 공통 래퍼 헬퍼 ────────────────────────────────────

def ok(agent: str, data: dict, reason: str = "") -> dict:
    return {"agent": agent, "status": "ok", "reason": reason, "data": data}

def error(agent: str, reason: str) -> dict:
    return {"agent": agent, "status": "error", "reason": reason, "data": {}}

def skipped(agent: str, reason: str) -> dict:
    return {"agent": agent, "status": "skipped", "reason": reason, "data": {}}


# ── Stub 에이전트들 ───────────────────────────────────
#
# 각 stub은 scenario 파라미터로 시나리오를 받아서
# bull / bear / neutral 세 가지 결과를 반환합니다.
# 실제 구현 시 이 함수들을 교체하면 오케스트레이터는 그대로 유지됩니다.

def call_dart_agent(tickers: list, scenario: str = "neutral") -> dict:
    """
    공시 분석 에이전트
    실제: dart_pipeline_agentic.py의 run() 호출
    """
    stub = {
        "bear": {
            "signal_score": -0.52,
            "confidence":   0.81,
            "opinion":      "SELL_BIAS",
            "trigger":      True,
            "urgency":      "immediate",
            "risk_level":   "high",
            "event_type":   "regulation",
            "horizon":      "short",
            "affected_tickers": ["005930"],
            "source_count": 3,
        },
        "bull": {
            "signal_score": 0.44,
            "confidence":   0.76,
            "opinion":      "BUY_BIAS",
            "trigger":      True,
            "urgency":      "scheduled",
            "risk_level":   "mid",
            "event_type":   "earnings",
            "horizon":      "short",
            "affected_tickers": ["005930", "000660"],
            "source_count": 2,
        },
        "neutral": {
            "signal_score": 0.08,
            "confidence":   0.62,
            "opinion":      "NEUTRAL",
            "trigger":      False,
            "urgency":      "watch",
            "risk_level":   "low",
            "event_type":   "earnings",
            "horizon":      "mid",
            "affected_tickers": ["005930"],
            "source_count": 1,
        },
    }
    return ok("dart_agent", stub[scenario],
               reason="공시 기반 시그널 분석 완료 (stub)")


def call_news_agent(tickers: list, scenario: str = "neutral") -> dict:
    """
    뉴스 분석 에이전트
    실제: 뉴스 크롤링 + Claude 감성 분석
    """
    stub = {
        "bear": {
            "signal_score": -0.38,
            "confidence":   0.70,
            "opinion":      "SELL_BIAS",
            "trigger":      True,
            "urgency":      "scheduled",
            "risk_level":   "mid",
            "event_type":   "regulation",
            "horizon":      "short",
            "affected_tickers": ["005930"],
            "source_count": 7,
        },
        "bull": {
            "signal_score": 0.31,
            "confidence":   0.68,
            "opinion":      "MILD_BUY",
            "trigger":      False,
            "urgency":      "watch",
            "risk_level":   "low",
            "event_type":   "product",
            "horizon":      "mid",
            "affected_tickers": ["000660"],
            "source_count": 5,
        },
        "neutral": {
            "signal_score": -0.05,
            "confidence":   0.55,
            "opinion":      "NEUTRAL",
            "trigger":      False,
            "urgency":      "watch",
            "risk_level":   "low",
            "event_type":   "macro",
            "horizon":      "long",
            "affected_tickers": [],
            "source_count": 4,
        },
    }
    return ok("news_agent", stub[scenario],
               reason="뉴스 감성 분석 완료 (stub)")


def call_report_agent(tickers: list, scenario: str = "neutral") -> dict:
    """
    증권사 리포트 분석 에이전트
    실제: 리포트 PDF 파싱 + Claude 분석
    """
    stub = {
        "bear": {
            "signal_score": -0.29,
            "confidence":   0.85,
            "opinion":      "MILD_SELL",
            "trigger":      False,
            "urgency":      "watch",
            "risk_level":   "low",
            "event_type":   "earnings",
            "horizon":      "mid",
            "affected_tickers": ["005930"],
            "source_count": 2,
        },
        "bull": {
            "signal_score": 0.58,
            "confidence":   0.88,
            "opinion":      "BUY_BIAS",
            "trigger":      True,
            "urgency":      "scheduled",
            "risk_level":   "mid",
            "event_type":   "earnings",
            "horizon":      "mid",
            "affected_tickers": ["005930", "000660"],
            "source_count": 3,
        },
        "neutral": {
            "signal_score": 0.12,
            "confidence":   0.72,
            "opinion":      "MILD_BUY",
            "trigger":      False,
            "urgency":      "watch",
            "risk_level":   "low",
            "event_type":   "earnings",
            "horizon":      "mid",
            "affected_tickers": ["005930"],
            "source_count": 2,
        },
    }
    return ok("report_agent", stub[scenario],
               reason="증권사 리포트 분석 완료 (stub)")


def call_profit_agent(rebalance_plan: dict, scenario: str = "neutral") -> dict:
    """
    수익 시뮬레이션 에이전트
    입력: rebalance_plan = {"005930": -0.10, "000660": +0.10}  # 비중 변화
    실제: 몬테카를로 시뮬레이션 or 팩터 모델
    """
    stub = {
        "bear": {
            "expected_return_1m": -2.1,    # %
            "expected_return_3m": -5.3,
            "sharpe_ratio":        0.41,
            "max_drawdown":       -8.2,
            "rebalance_plan":     rebalance_plan,
            "recommendation":     "리밸런싱 후 기대수익 개선 예상",
        },
        "bull": {
            "expected_return_1m":  3.8,
            "expected_return_3m":  9.1,
            "sharpe_ratio":        1.23,
            "max_drawdown":       -3.1,
            "rebalance_plan":     rebalance_plan,
            "recommendation":     "현 비중 유지 또는 확대 고려",
        },
        "neutral": {
            "expected_return_1m":  0.9,
            "expected_return_3m":  2.4,
            "sharpe_ratio":        0.87,
            "max_drawdown":       -4.5,
            "rebalance_plan":     rebalance_plan,
            "recommendation":     "소폭 조정으로 리스크 개선 가능",
        },
    }
    return ok("profit_agent", stub[scenario],
               reason="수익 시뮬레이션 완료 (stub)")


def call_cost_agent(rebalance_plan: dict,
                    total_value_krw: int,
                    scenario: str = "neutral") -> dict:
    """
    세금/수수료 계산 에이전트
    입력: rebalance_plan = {"005930": -0.10, "000660": +0.10}
    실제: 양도세 계산 + 증권사 수수료 테이블 적용
    """
    trade_amount = sum(
        abs(v) * total_value_krw
        for v in rebalance_plan.values()
    )

    # 간단한 stub 계산 (실제는 취득가액 기반 양도세 계산 필요)
    commission = round(trade_amount * 0.00015, 0)   # 0.015% 가정
    tax        = round(trade_amount * 0.0023,  0)   # 증권거래세 0.23%
    total_cost = commission + tax
    cost_ratio = round(total_cost / total_value_krw * 100, 3)

    return ok("cost_agent", {
        "trade_amount_krw": int(trade_amount),
        "commission_krw":   int(commission),
        "tax_krw":          int(tax),
        "total_cost_krw":   int(total_cost),
        "cost_ratio_pct":   cost_ratio,
        "note":             "stub — 실제 취득가액 기반 양도세 계산 필요",
    }, reason="거래비용 계산 완료 (stub)")


# ── 오케스트레이터 시스템 프롬프트 ───────────────────

ORCHESTRATOR_SYSTEM = """You are a portfolio rebalancing orchestrator for a Korean stock portfolio.

You have access to the following agents as tools:
  - call_dart_agent    : 공시 분석 (regulatory filings signal)
  - call_news_agent    : 뉴스 감성 분석
  - call_report_agent  : 증권사 리포트 분석
  - call_profit_agent  : 리밸런싱 후 수익 시뮬레이션 (방향 결정 후 호출)
  - call_cost_agent    : 세금/수수료 계산 (방향 결정 후 호출)

Decision flow:
  1. Start with signal agents (dart, news, report) — call only what's needed
  2. Once you have enough signal confidence, determine rebalance direction
  3. Then call profit_agent and cost_agent to validate the decision
  4. Make final rebalancing decision

Output your final decision as JSON:
{
  "decision":        "REBALANCE" | "HOLD",
  "urgency":         "immediate" | "scheduled" | "watch",
  "rebalance_plan":  {"005930": <weight_delta>, "000660": <weight_delta>},
  "confidence":      <0.0 to 1.0>,
  "reason":          "<Korean 2~3 sentences>",
  "agents_called":   ["dart_agent", ...]
}"""


ORCHESTRATOR_TOOLS = [
    {
        "name": "call_dart_agent",
        "description": "공시(DART) 기반 시그널 분석. 종목별 규제·실적·인사 관련 공시를 분석합니다. 불확실성이 높거나 규제 이슈가 의심될 때 우선 호출하세요.",
        "input_schema": {
            "type": "object",
            "properties": {
                "tickers": {"type": "array", "items": {"type": "string"}, "description": "분석할 종목코드 목록"}
            },
            "required": ["tickers"]
        }
    },
    {
        "name": "call_news_agent",
        "description": "실시간 뉴스 감성 분석. 단기 시장 반응 예측에 유용합니다. dart 시그널이 불확실하거나 추가 확인이 필요할 때 호출하세요.",
        "input_schema": {
            "type": "object",
            "properties": {
                "tickers": {"type": "array", "items": {"type": "string"}, "description": "분석할 종목코드 목록"}
            },
            "required": ["tickers"]
        }
    },
    {
        "name": "call_report_agent",
        "description": "증권사 리포트 분석. 중장기 전망과 목표주가 기반 시그널을 제공합니다. 중장기 판단이 필요할 때 호출하세요.",
        "input_schema": {
            "type": "object",
            "properties": {
                "tickers": {"type": "array", "items": {"type": "string"}, "description": "분석할 종목코드 목록"}
            },
            "required": ["tickers"]
        }
    },
    {
        "name": "call_profit_agent",
        "description": "리밸런싱 후 기대수익 시뮬레이션. 반드시 리밸런싱 방향(rebalance_plan)이 결정된 후에만 호출하세요.",
        "input_schema": {
            "type": "object",
            "properties": {
                "rebalance_plan": {
                    "type": "object",
                    "description": "종목별 비중 변화 (예: {\"005930\": -0.10, \"000660\": 0.10})"
                }
            },
            "required": ["rebalance_plan"]
        }
    },
    {
        "name": "call_cost_agent",
        "description": "거래 세금/수수료 계산. 반드시 리밸런싱 방향(rebalance_plan)이 결정된 후에만 호출하세요.",
        "input_schema": {
            "type": "object",
            "properties": {
                "rebalance_plan": {
                    "type": "object",
                    "description": "종목별 비중 변화 (예: {\"005930\": -0.10, \"000660\": 0.10})"
                }
            },
            "required": ["rebalance_plan"]
        }
    },
]


# ── Mock 오케스트레이터 (API 키 없이 테스트) ──────────
#
# 실제 Claude 대신 시나리오별 고정 호출 순서를 흉내냅니다.
# 오케스트레이터 로직(도구 실행, 결과 수집, 최종 판단)이
# 실제와 동일하게 동작하는지 확인할 수 있습니다.

def mock_orchestrator_decisions(scenario: str) -> list[dict]:
    """
    시나리오별로 Claude가 내릴 법한 도구 호출 결정을 흉내냄
    실제 Claude API 호출 시 이 부분이 agentic_loop()로 교체됩니다.
    """
    if scenario == "bear":
        # 부정적 시나리오: dart 확인 → 뉴스 추가 확인 → 방향 결정 → 비용 검토
        return [
            {"tool": "call_dart_agent",   "inputs": {"tickers": ["005930", "000660"]}},
            {"tool": "call_news_agent",   "inputs": {"tickers": ["005930"]}},
            {"tool": "call_cost_agent",   "inputs": {"rebalance_plan": {"005930": -0.15, "000660": 0.15}}},
            {"tool": "call_profit_agent", "inputs": {"rebalance_plan": {"005930": -0.15, "000660": 0.15}}},
        ]
    elif scenario == "bull":
        # 긍정적 시나리오: dart만으로 충분 → 수익 확인
        return [
            {"tool": "call_dart_agent",   "inputs": {"tickers": ["005930", "000660"]}},
            {"tool": "call_profit_agent", "inputs": {"rebalance_plan": {"005930": 0.10, "000660": -0.10}}},
        ]
    else:
        # 중립: dart + 리포트 확인 → HOLD
        return [
            {"tool": "call_dart_agent",   "inputs": {"tickers": ["005930", "000660"]}},
            {"tool": "call_report_agent", "inputs": {"tickers": ["005930", "000660"]}},
        ]


def run_mock_orchestrator(portfolio: dict, scenario: str) -> dict:
    """
    Mock 오케스트레이터 실행
    Claude 호출 없이 오케스트레이터 전체 흐름을 테스트합니다.
    """
    print(f"\n{'='*60}")
    print(f"[MOCK 오케스트레이터] 시나리오: {scenario.upper()}")
    print(f"포트폴리오: {portfolio['weights']}")
    print(f"{'='*60}")

    decisions   = mock_orchestrator_decisions(scenario)
    agent_results = {}

    # 도구 순서대로 실행
    for step in decisions:
        tool   = step["tool"]
        inputs = step["inputs"]
        print(f"\n→ {tool}({inputs})")

        if tool == "call_dart_agent":
            result = call_dart_agent(inputs["tickers"], scenario)
        elif tool == "call_news_agent":
            result = call_news_agent(inputs["tickers"], scenario)
        elif tool == "call_report_agent":
            result = call_report_agent(inputs["tickers"], scenario)
        elif tool == "call_profit_agent":
            result = call_profit_agent(
                inputs["rebalance_plan"], scenario)
        elif tool == "call_cost_agent":
            result = call_cost_agent(
                inputs["rebalance_plan"],
                portfolio["total_value_krw"],
                scenario
            )
        else:
            result = error(tool, "알 수 없는 도구")

        agent_results[tool] = result

        # 결과 출력
        status = result["status"]
        if status == "ok":
            data = result["data"]
            if "signal_score" in data:
                print(f"  signal_score={data['signal_score']:+.2f} "
                      f"opinion={data['opinion']} "
                      f"confidence={data['confidence']:.2f}")
            elif "expected_return_1m" in data:
                print(f"  기대수익(1m)={data['expected_return_1m']:+.1f}% "
                      f"sharpe={data['sharpe_ratio']:.2f}")
            elif "total_cost_krw" in data:
                print(f"  거래비용=₩{data['total_cost_krw']:,} "
                      f"({data['cost_ratio_pct']:.3f}%)")
        else:
            print(f"  [{status}] {result['reason']}")

    # 최종 판단 (mock — 실제는 Claude가 판단)
    final = _mock_final_decision(agent_results, scenario)

    print(f"\n{'─'*60}")
    print(f"[최종 판단]")
    print(f"  decision       : {final['decision']}")
    print(f"  urgency        : {final['urgency']}")
    print(f"  rebalance_plan : {final['rebalance_plan']}")
    print(f"  confidence     : {final['confidence']:.2f}")
    print(f"  reason         : {final['reason']}")
    print(f"  agents_called  : {final['agents_called']}")
    print(f"{'='*60}")

    return final


def _mock_final_decision(results: dict, scenario: str) -> dict:
    """
    수집된 에이전트 결과를 바탕으로 최종 판단 생성 (mock)
    실제 Claude는 이 판단을 스스로 내립니다.
    """
    agents_called = list(results.keys())

    if scenario == "bear":
        return {
            "decision":       "REBALANCE",
            "urgency":        "immediate",
            "rebalance_plan": {"005930": -0.15, "000660": 0.15},
            "confidence":     0.78,
            "reason":         "공시 및 뉴스에서 005930 관련 규제 리스크 감지. "
                              "거래비용 대비 리스크 감소 효과가 커 비중 축소를 권고합니다.",
            "agents_called":  agents_called,
        }
    elif scenario == "bull":
        return {
            "decision":       "REBALANCE",
            "urgency":        "scheduled",
            "rebalance_plan": {"005930": 0.10, "000660": -0.10},
            "confidence":     0.71,
            "reason":         "공시 시그널이 긍정적이며 기대수익 개선이 예상됩니다. "
                              "005930 비중 확대를 권고합니다.",
            "agents_called":  agents_called,
        }
    else:
        return {
            "decision":       "HOLD",
            "urgency":        "watch",
            "rebalance_plan": {},
            "confidence":     0.65,
            "reason":         "공시 및 리포트 시그널 모두 중립 수준입니다. "
                              "현재 비중을 유지하며 추가 모니터링을 권고합니다.",
            "agents_called":  agents_called,
        }


# ── 실제 Claude API 오케스트레이터 ───────────────────
#
# API 키가 생기면 이 함수가 mock을 대체합니다.
# mock과 인터페이스(입력/출력)가 동일하므로 run() 함수는 수정 불필요.

def run_real_orchestrator(portfolio: dict, scenario: str) -> dict:
    """
    실제 Claude API 기반 오케스트레이터
    --real 플래그로 실행 시 사용
    """
    import anthropic
    client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

    user_prompt = f"""Current portfolio state:
{json.dumps(portfolio, ensure_ascii=False, indent=2)}

Today's date: {datetime.now(timezone.utc).strftime('%Y-%m-%d')}

Analyze the portfolio and determine if rebalancing is needed.
Call the necessary agents to gather information, then provide your final decision as JSON."""

    messages = [{"role": "user", "content": user_prompt}]
    agent_results = {}
    total_input = total_output = 0

    print(f"\n{'='*60}")
    print(f"[REAL 오케스트레이터] Claude API 호출 중...")
    print(f"{'='*60}")

    MAX_ROUNDS = 8
    for _ in range(MAX_ROUNDS):
        resp = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=2048,
            system=ORCHESTRATOR_SYSTEM,
            tools=ORCHESTRATOR_TOOLS,
            messages=messages,
        )
        total_input  += resp.usage.input_tokens
        total_output += resp.usage.output_tokens

        if resp.stop_reason == "tool_use":
            messages.append({"role": "assistant", "content": resp.content})
            tool_results = []

            for block in resp.content:
                if block.type != "tool_use":
                    continue

                print(f"\n→ {block.name}({block.input})")
                inputs = block.input

                # 실제 stub 에이전트 호출
                if block.name == "call_dart_agent":
                    result = call_dart_agent(inputs["tickers"], scenario)
                elif block.name == "call_news_agent":
                    result = call_news_agent(inputs["tickers"], scenario)
                elif block.name == "call_report_agent":
                    result = call_report_agent(inputs["tickers"], scenario)
                elif block.name == "call_profit_agent":
                    result = call_profit_agent(inputs["rebalance_plan"], scenario)
                elif block.name == "call_cost_agent":
                    result = call_cost_agent(
                        inputs["rebalance_plan"],
                        portfolio["total_value_krw"],
                        scenario
                    )
                else:
                    result = error(block.name, "알 수 없는 도구")

                agent_results[block.name] = result
                result_str = json.dumps(result, ensure_ascii=False)
                print(f"  → {result_str[:120]}...")

                tool_results.append({
                    "type":        "tool_result",
                    "tool_use_id": block.id,
                    "content":     result_str,
                })

            messages.append({"role": "user", "content": tool_results})

        elif resp.stop_reason == "end_turn":
            final_text = "".join(
                b.text for b in resp.content if hasattr(b, "text")
            )
            clean = final_text.replace("```json", "").replace("```", "").strip()
            final = json.loads(clean)

            print(f"\n{'─'*60}")
            print(f"[최종 판단]")
            print(f"  decision       : {final['decision']}")
            print(f"  urgency        : {final['urgency']}")
            print(f"  rebalance_plan : {final.get('rebalance_plan', {})}")
            print(f"  confidence     : {final['confidence']:.2f}")
            print(f"  reason         : {final['reason']}")
            print(f"  agents_called  : {final.get('agents_called', [])}")
            print(f"토큰: 입력 {total_input:,} / 출력 {total_output:,}")
            print(f"{'='*60}")
            return final

    raise RuntimeError("MAX_ROUNDS 초과")


# ── 진입점 ────────────────────────────────────────────

def run(real: bool = False, scenario: str = "neutral"):
    if real:
        return run_real_orchestrator(PORTFOLIO_STATE, scenario)
    else:
        return run_mock_orchestrator(PORTFOLIO_STATE, scenario)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="포트폴리오 리밸런싱 오케스트레이터")
    parser.add_argument("--real",     action="store_true",
                        help="실제 Claude API 호출 (기본: mock 모드)")
    parser.add_argument("--scenario", default="neutral",
                        choices=["bull", "bear", "neutral"],
                        help="테스트 시나리오 (기본: neutral)")
    args = parser.parse_args()

    run(real=args.real, scenario=args.scenario)