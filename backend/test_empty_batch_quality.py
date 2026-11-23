"""空批次质量诊断回归测试。

场景：同一日志源的两个解析批次，一个有解析错误，另一个输入已清空（零行）。
要求：空批次判“确认零行”，旧错误只作历史证据，不改变当前批次的成功率/错误数。
"""
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

import app.services.base as base  # noqa: E402
from app.services.log_parser import LogParserService  # noqa: E402
from app.services.log_quality import LogQualityService  # noqa: E402
from app.store import store  # noqa: E402

SOURCE = "LOG_-9001"


@pytest.fixture()
def data_root(tmp_path, monkeypatch):
    """每个用例独立的数据根目录，避免批次产物互相污染。"""
    root = tmp_path / "data"
    for sub in ("logs", "exports", "checkpoints", "events", "aggregations"):
        (root / sub).mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(base, "DATA_ROOT", root)
    yield root


@pytest.fixture()
def parser():
    # 让内置默认正则生效：不启用自定义规则，坏行才会真的解析失败。
    for row in store.rows("log_parser"):
        row["status"] = "已停用"
    return LogParserService()


def _input_file(parser: LogParserService, root: Path) -> Path:
    return root / "logs" / f"{SOURCE}.jsonl"


def _write_lines(parser: LogParserService, path: Path, raws: list[str]) -> None:
    parser._replace_jsonl(path, [
        {"source_key": SOURCE, "line_no": i + 1, "raw": raw, "collected_at": 1700000000 + i}
        for i, raw in enumerate(raws)
    ])


def test_empty_batch_does_not_inherit_previous_errors(data_root, parser):
    quality = LogQualityService()
    log_file = _input_file(parser, data_root)

    # 第一批：两行坏数据
    _write_lines(parser, log_file, ["bad-line-1", "bad-line-2"])
    first = parser.parse_log_file(SOURCE)
    first_report = quality.diagnose_source(SOURCE)
    assert first_report["verdict"] == LogQualityService.VERDICT_HAS_ERRORS
    assert (first_report["input_lines"], first_report["parsed_ok"], first_report["parse_errors"]) == (2, 0, 2)
    assert len(first_report["current_batch"]["error_lines"]) == 2

    # 清空本批输入，再解析同源：应生成新的零行批次
    log_file.write_text("")
    second = parser.parse_log_file(SOURCE)
    empty_report = quality.diagnose_source(SOURCE)

    assert second["batch_id"] != first["batch_id"]
    assert empty_report["batch_id"] == second["batch_id"]
    assert empty_report["verdict"] == LogQualityService.VERDICT_CONFIRMED_EMPTY
    assert empty_report["completed"] is True
    assert empty_report["input_lines"] == 0
    assert empty_report["parse_errors"] == 0
    assert empty_report["parse_success_rate"] == 100.0
    assert empty_report["current_batch"]["error_lines"] == []
    assert empty_report["dirty_data_types"]["total"] == 0

    # 历史异常仍可查：旧批次的错误原行保留在历史区，但明确不计入当前批次
    assert empty_report["history"]["excluded_from_current"] is True
    assert empty_report["history"]["batches_with_errors"] == 1
    historical = empty_report["history"]["batches"][0]
    assert historical["batch_id"] == first["batch_id"]
    assert [line["raw"] for line in historical["error_lines"]] == ["bad-line-1", "bad-line-2"]

    # 历史批次的原始证据也能按批次号单独取回
    stored = parser.get_batch(SOURCE, first["batch_id"])
    assert stored is not None
    assert stored["is_current"] is False
    assert len(stored["error_records"]) == 2


def test_current_report_is_overwritten_and_scoped_to_current_batch(data_root, parser):
    quality = LogQualityService()
    log_file = _input_file(parser, data_root)

    _write_lines(parser, log_file, ["bad-line-1"])
    parser.parse_log_file(SOURCE)
    quality.diagnose_source(SOURCE)

    log_file.write_text("")
    second = parser.parse_log_file(SOURCE)
    quality.diagnose_source(SOURCE)

    current = quality.get_current_report(SOURCE)
    assert current is not None
    assert current["batch_id"] == second["batch_id"]
    assert current["verdict"] == LogQualityService.VERDICT_CONFIRMED_EMPTY
    assert current["parse_errors"] == 0
    assert current["history"]["batches_with_errors"] == 1

    report_file = data_root / "exports" / f"quality_current_{SOURCE}.json"
    on_disk = json.loads(report_file.read_text(encoding="utf-8"))
    assert on_disk["batch_id"] == second["batch_id"]


def test_interrupted_parse_is_pending_not_confirmed_empty(data_root, parser):
    quality = LogQualityService()
    log_file = _input_file(parser, data_root)
    _write_lines(parser, log_file, ["bad-line-1", "bad-line-2"])

    # 解析过程断开：批次登记为已中断，产物只完成了一部分（这里为空）
    batch = parser.begin_batch(SOURCE)
    parser.mark_batch_interrupted(SOURCE, batch["batch_id"], "解析进程断开")
    report = quality.diagnose_source(SOURCE)

    assert report["verdict"] == LogQualityService.VERDICT_PENDING
    assert report["completed"] is False
    assert report["parse_success_rate"] is None
    assert report["retry"]["retryable"] is True
    assert report["retry"]["reason"] == "解析进程断开"
    assert report["retry"]["source_batch_id"] == batch["batch_id"]
    assert report["retry"]["input_file"].endswith(f"{SOURCE}.jsonl")
    assert "auto_alert" not in report

    # 重新解析后当前批次变为完成口径，待核结论让位给真实结果
    parser.parse_log_file(SOURCE)
    resolved = quality.diagnose_source(SOURCE)
    assert resolved["verdict"] == LogQualityService.VERDICT_HAS_ERRORS
    assert resolved["parse_success_rate"] == 0.0
    assert resolved["retry"] is None


def test_missing_artifact_differs_from_empty_input(data_root, parser):
    quality = LogQualityService()
    log_file = _input_file(parser, data_root)

    # 有输入但产物丢失：缺产物
    _write_lines(parser, log_file, ["bad-line-1"])
    batch = parser.parse_log_file(SOURCE)
    (parser.batch_dir(SOURCE, batch["batch_id"]) / "parsed.jsonl").unlink()
    missing = quality.diagnose_source(SOURCE)
    assert missing["verdict"] == LogQualityService.VERDICT_MISSING
    assert missing["current_batch"]["missing_artifacts"] == ["parsed.jsonl"]
    assert missing["completed"] is False
    assert missing["retry"]["retryable"] is True

    # 输入文件存在但为 0 行：确认零行（与缺产物判定不同）
    parser.parse_log_file(SOURCE)  # 重新落一份完整产物
    log_file.write_text("")
    parser.parse_log_file(SOURCE)
    empty = quality.diagnose_source(SOURCE)
    assert empty["verdict"] == LogQualityService.VERDICT_CONFIRMED_EMPTY
    assert empty["completed"] is True


def test_quality_pipeline_skips_alert_for_pending_batch(data_root, parser):
    from app.services.pipeline import CrossModulePipeline

    log_file = _input_file(parser, data_root)
    _write_lines(parser, log_file, ["bad-line-1"])
    batch = parser.begin_batch(SOURCE)
    parser.mark_batch_interrupted(SOURCE, batch["batch_id"], "解析进程断开")

    result = CrossModulePipeline().quality_alert_pipeline(SOURCE)
    assert result["stages"]["diagnose"]["verdict"] == LogQualityService.VERDICT_PENDING
    assert result["stages"]["alert"]["skipped"] is True
    assert result["stages"]["alert"]["retry"]["retryable"] is True
    assert "rollback" not in result["stages"]
