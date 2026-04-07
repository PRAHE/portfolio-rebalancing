"""
DART 공시 데이터 수집기
목적: 파인튜닝 학습 데이터 구축

설치:
    pip install python-dotenv OpenDartReader pandas pyarrow tqdm

실행:
    python dart_collector.py                    # 전체 상장사, 2015~현재
    python dart_collector.py --test             # 삼성전자만 테스트
    python dart_collector.py --start 2020       # 2020년부터
    python dart_collector.py --kinds B          # 주요사항보고만
"""

import os
import re
import time
import hashlib
import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()

try:
    import OpenDartReader
    dart = OpenDartReader(os.environ["DART_API_KEY"])
except KeyError:
    raise EnvironmentError(".env에 DART_API_KEY가 없습니다.")
except ImportError:
    raise ImportError("pip install OpenDartReader 를 먼저 실행하세요.")


# ── 설정 ──────────────────────────────────────────────

KIND_LABEL = {
    'A': '공시-정기',
    'B': '공시-주요사항',
    'C': '공시-발행',
    'D': '공시-지분',
}

# 수집 우선순위 (B → A → C → D)
DEFAULT_KINDS = ['B', 'A', 'C', 'D']

OUTPUT_DIR = Path("dart_data")
OUTPUT_DIR.mkdir(exist_ok=True)

FAILED_LOG = OUTPUT_DIR / "failed.jsonl"   # 실패 로그
PROGRESS_LOG = OUTPUT_DIR / "progress.json"  # 진행 상황 (중단 후 재개용)


# ── 텍스트 추출 ───────────────────────────────────────

def extract_text(raw: str, max_chars: int = 500) -> str:
    """XML/HTML 태그 제거 후 앞부분만 추출"""
    text = re.sub(r'<[^>]+>', ' ', raw)
    text = re.sub(r'\s+', ' ', text).strip()
    # 의미 없는 짧은 텍스트 걸러냄
    if len(text) < 10:
        return ""
    return text[:max_chars]


def parse_date(rcept_dt: str) -> str:
    """20150101 → 2015-01-01T00:00:00Z"""
    try:
        return datetime.strptime(str(rcept_dt), "%Y%m%d").strftime("%Y-%m-%dT00:00:00Z")
    except Exception:
        return ""


# ── 공시 수집 ─────────────────────────────────────────

def collect_filings(ticker: str, corp_name: str,
                    start: str, end: str,
                    kinds: list[str]) -> list[dict]:
    """
    종목 하나의 공시를 모든 kind에 대해 수집
    반환: normalized_content 포함한 dict 리스트
    """
    results = []

    for kind in kinds:
        print(f"\n  [{ticker}] kind={kind} 목록 조회 중...", flush=True)
        try:
            df = dart.list(
                ticker,
                start=start,
                end=end,
                kind=kind,
                final=True
            )
        except Exception as e:
            print(f"  [{ticker}] kind={kind} 실패: {e}", flush=True)
            _log_failed(ticker, kind, "", str(e))
            continue

        if df is None or df.empty:
            print(f"  [{ticker}] kind={kind} 결과 없음", flush=True)
            continue

        print(f"  [{ticker}] kind={kind} {len(df)}건 발견", flush=True)

        for idx, (_, row) in enumerate(df.iterrows()):
            rcept_no = row.get("rcept_no", "")
            title    = row.get("report_nm", "")
            print(f"    {idx+1}/{len(df)} {title[:40]}", flush=True)

            # 본문 추출
            body = ""
            try:
                doc  = dart.document(rcept_no)
                body = extract_text(doc)
            except Exception as e:
                print(f"    본문 추출 실패: {e}", flush=True)
                _log_failed(ticker, kind, rcept_no, str(e))
                # 본문 실패해도 제목만으로 계속 진행

            doc_id = hashlib.md5(rcept_no.encode()).hexdigest()

            results.append({
                # ── 파인튜닝 입력 포맷 ──
                "normalized_content": {
                    "title":        row.get("report_nm", ""),
                    "body":         body,
                    "type":         KIND_LABEL.get(kind, "공시"),
                    "published_at": parse_date(str(row.get("rcept_dt", ""))),
                },
                # ── 파이프라인 메타 ──
                "id":           doc_id,
                "rcept_no":     rcept_no,
                "source_type":  "dart",
                "source_name":  "DART",
                "corp_name":    corp_name,
                "tickers":      [ticker],
                "trust_score":  1.0,
                "kind":         kind,
                "url":          f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={rcept_no}",
                "collected_at": datetime.now(timezone.utc).isoformat(),
            })

            time.sleep(0.3)   # API 부하 방지

    return results


def _log_failed(ticker: str, kind: str, rcept_no: str, reason: str):
    """실패 케이스 로깅"""
    with open(FAILED_LOG, "a", encoding="utf-8") as f:
        f.write(json.dumps({
            "ticker":   ticker,
            "kind":     kind,
            "rcept_no": rcept_no,
            "reason":   reason,
            "at":       datetime.now(timezone.utc).isoformat(),
        }, ensure_ascii=False) + "\n")


# ── 진행 상황 저장/복원 (중단 후 재개용) ────────────────

def load_progress() -> set:
    """이미 수집 완료된 ticker 목록"""
    if PROGRESS_LOG.exists():
        with open(PROGRESS_LOG, "r") as f:
            return set(json.load(f).get("done", []))
    return set()


def save_progress(done: set):
    with open(PROGRESS_LOG, "w") as f:
        json.dump({"done": list(done), "updated_at": datetime.now().isoformat()}, f)


# ── 전체 실행 ─────────────────────────────────────────

def run(start_year: int = 2015,
        kinds: list[str] = None,
        test_mode: bool = False):

    kinds = kinds or DEFAULT_KINDS
    start = f"{start_year}0101"
    end   = datetime.now().strftime("%Y%m%d")

    # 전체 상장사 목록
    corp_list = dart.corp_codes
    listed    = corp_list[corp_list["stock_code"].notna()].copy()

    if test_mode:
        # 삼성전자 + SK하이닉스만
        listed = listed[listed["stock_code"].isin(["005930", "000660"])]
        print(f"[테스트 모드] {len(listed)}개 종목")
    else:
        print(f"전체 상장사: {len(listed)}개 종목")

    # 중단 후 재개
    done = load_progress()
    remaining = listed[~listed["stock_code"].isin(done)]
    print(f"수집 대상: {len(remaining)}개 (완료: {len(done)}개)")

    all_rows = []
    batch_size = 100  # 100개 종목마다 Parquet 저장

    for i, (_, corp) in enumerate(tqdm(remaining.iterrows(), total=len(remaining))):
        ticker    = corp["stock_code"]
        corp_name = corp.get("corp_name", "")

        rows = collect_filings(ticker, corp_name, start, end, kinds)
        all_rows.extend(rows)

        done.add(ticker)

        # 배치 저장
        if len(all_rows) >= batch_size or i == len(remaining) - 1:
            _save_batch(all_rows, start_year)
            all_rows = []
            save_progress(done)
            print(f"\n저장 완료 — 누적 {len(done)}개 종목")

        time.sleep(0.5)   # 종목 간 딜레이

    print("\n수집 완료")
    _merge_parquet(start_year)


def _save_batch(rows: list[dict], start_year: int):
    """배치 단위로 Parquet 저장"""
    if not rows:
        return

    # normalized_content를 컬럼으로 펼치기
    records = []
    for r in rows:
        nc = r.pop("normalized_content", {})
        records.append({**r, **nc})

    df   = pd.DataFrame(records)
    ts   = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = OUTPUT_DIR / f"dart_{start_year}_{ts}.parquet"
    df.to_parquet(path, index=False, compression="gzip")
    print(f"\n배치 저장: {path} ({len(df)}건)")


def _merge_parquet(start_year: int):
    """수집 완료 후 전체 Parquet 파일 병합"""
    files = list(OUTPUT_DIR.glob(f"dart_{start_year}_*.parquet"))
    if not files:
        return

    dfs   = [pd.read_parquet(f) for f in files]
    final = pd.concat(dfs, ignore_index=True)

    # 중복 제거 (id 기준)
    final = final.drop_duplicates(subset=["id"])

    out = OUTPUT_DIR / f"dart_{start_year}_full.parquet"
    final.to_parquet(out, index=False, compression="gzip")
    print(f"\n최종 병합 완료: {out}")
    print(f"총 {len(final):,}건 / {final['kind'].value_counts().to_dict()}")


# ── 진입점 ────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DART 공시 데이터 수집기")
    parser.add_argument("--start", type=int, default=2020,   help="수집 시작 연도 (기본: 2015)")
    parser.add_argument("--kinds", nargs="+", default=None,  help="수집할 kind (기본: B A C D)")
    parser.add_argument("--test",  action="store_true",      help="테스트 모드 (삼성전자+SK하이닉스만)")
    args = parser.parse_args()

    run(
        start_year=args.start,
        kinds=args.kinds,
        test_mode=args.test,
    )