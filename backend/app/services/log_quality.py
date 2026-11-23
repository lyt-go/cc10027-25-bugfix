"""质量诊断：只针对解析端的当前批次生成可复查报告。

诊断范围严格限定在“当前批次”：
- 当前批次解析完成且输入为 0 行 → 判定“确认零行”，成功率不拿历史错误凑数；
- 当前批次中断/未完成 → 判定“待核”，给出重试依据，但不出具完成结论；
- 当前批次产物缺失 → 判定“缺产物”，与“空输入”区分开；
- 历史批次中的错误只在历史区展示，并标明不计入当前批次。
"""
from __future__ import annotations

import math
import statistics
import time
from collections import defaultdict
from typing import Any

from app.services.base import BaseModuleService
from app.services.log_parser import LogParserService
from app.store import store


class LogQualityService(BaseModuleService):
    MODULE = "log_quality"
    ENTITY = "诊断报告"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["报告编号", "关联源", "判定", "解析成功率", "字段完整性", "延迟分布", "脏数据类型", "诊断时间", "报告状态"]
    REQUIRED_FIELDS = ["报告编号", "关联源", "解析成功率"]
    STATUS_ORDER = ["诊断中", "已生成", "已复核", "已归档"]
    ACTION_RULES = {"确认生成": "已生成", "标记复核": "已复核", "提交归档": "已归档"}
    NEGATIVE_ACTIONS = []

    VERDICT_COMPLETED = "解析完成"
    VERDICT_HAS_ERRORS = "存在解析错误"
    VERDICT_CONFIRMED_EMPTY = "确认零行"
    VERDICT_PENDING = "待核"
    VERDICT_MISSING = "缺产物"

    # 中断/未完成时可用于重试的状态（批次仍可能被重新跑一遍）
    PENDING_BATCH_STATUSES = {"进行中", "已中断", "已失败"}

    def diagnose_source(self, source_key: str) -> dict[str, Any]:
        """对指定日志源的“当前批次”进行质量诊断。"""
        parser = LogParserService()
        current = parser.get_current_batch(source_key)
        report_id = f"QL_{source_key}_{int(time.time() * 1000)}"
        diagnosed_at = time.time()

        verdict, reason, batch_ctx = self._classify_current_batch(parser, source_key, current)

        batch_id = (current or {}).get("batch_id")
        parsed_records: list[dict[str, Any]] = []
        error_records: list[dict[str, Any]] = []
        input_lines = 0

        if batch_ctx["readable"]:
            bdir = parser.batch_dir(source_key, batch_id or "")
            parsed_records = self._read_jsonl(bdir / "parsed.jsonl")
            error_records = self._read_jsonl(bdir / "parse_errors.jsonl")
            manifest = batch_ctx.get("manifest") or {}
            input_lines = int(manifest.get("input_lines", len(parsed_records) + len(error_records)))

        parsed_ok = len(parsed_records)
        parse_errors = len(error_records)

        if verdict in (self.VERDICT_PENDING, self.VERDICT_MISSING):
            # 未完成/缺产物时不出具成功率这种完成口径，也不能用历史证据填补。
            success_rate = None
        elif input_lines <= 0:
            success_rate = 100.0
        else:
            success_rate = round(parsed_ok / max(input_lines, 1) * 100, 2)

        completeness = self._compute_completeness(parsed_records)
        latency_dist = self._compute_latency_distribution(parsed_records)
        dirty_types = self._classify_dirty_data(error_records)
        history = self._history_without_current(parser, source_key, batch_id)

        report = {
            "report_id": report_id,
            "source_key": source_key,
            "batch_id": batch_id,
            "batch_status": (current or {}).get("status"),
            "diagnosed_at": diagnosed_at,
            "verdict": verdict,
            "verdict_reason": reason,
            "report_scope": "当前批次",
            "completed": verdict in (self.VERDICT_COMPLETED, self.VERDICT_HAS_ERRORS, self.VERDICT_CONFIRMED_EMPTY),
            "input_lines": input_lines,
            "parsed_ok": parsed_ok,
            "parse_errors": parse_errors,
            "parse_success_rate": success_rate,
            "total_records": input_lines,
            "field_completeness": completeness,
            "latency_distribution": latency_dist,
            "dirty_data_types": dirty_types,
            "current_batch": {
                "batch_id": batch_id,
                "status": (current or {}).get("status"),
                "input_file": (batch_ctx.get("manifest") or {}).get("artifacts", {}).get("input"),
                "input_lines": input_lines,
                "input_signature": (batch_ctx.get("manifest") or {}).get("input_signature"),
                "parsed_file": self._relative_path(parser.batch_dir(source_key, batch_id or "") / "parsed.jsonl") if batch_id else None,
                "error_file": self._relative_path(parser.batch_dir(source_key, batch_id or "") / "parse_errors.jsonl") if batch_id else None,
                "error_lines": self._error_evidence(error_records),
                "missing_artifacts": batch_ctx.get("missing_artifacts", []),
                "evidence_note": "error_lines 仅来自当前批次；历史批次见 history.excluded_from_current",
            },
            "history": history,
            "history_note": "历史批次仅用于追溯旧异常，不计入当前批次判定",
            "retry": batch_ctx.get("retry"),
        }

        # 当前报告：固定文件名覆盖写，复查时读到的永远是最近一次诊断。
        current_report_file = self._data_dir("exports") / f"quality_current_{source_key}.json"
        self._write_json_file(current_report_file, report)
        # 诊断留档：每次诊断追加一行，便于趋势对比。
        self._write_jsonl(
            self._data_dir("exports") / "quality_reports.jsonl",
            [self._archive_view(report)],
        )
        report["current_report_file"] = self._relative_path(current_report_file)

        self._write_checkpoint(f"quality_{source_key}", report)
        self._publish_report_row(report)

        # 只有完成口径的诊断才自动评估告警；待核/缺产物走重试，不拿历史错误触发告警。
        if report["completed"] and (success_rate is not None and success_rate < 95 or parse_errors > 0):
            alert_r = self._trigger_alert_evaluation(source_key, {
                "parse_success_rate": success_rate,
                "field_completeness": completeness.get("overall", 100),
                "parse_errors": parse_errors,
                "source_key": source_key,
                "batch_id": batch_id,
            })
            if "error" not in alert_r:
                report["auto_alert"] = alert_r

        return report

    # -- 当前批次判定 --

    def _classify_current_batch(
        self,
        parser: LogParserService,
        source_key: str,
        current: dict[str, Any] | None,
    ) -> tuple[str, str, dict[str, Any]]:
        if current is None:
            return (
                self.VERDICT_MISSING,
                "当前没有可复查的解析批次（从未解析或批次指针缺失），不能套用历史批次的解析结果",
                {
                    "readable": False,
                    "retry": self._retry_basis(source_key, None, "无当前批次产物"),
                },
            )

        batch_id = current.get("batch_id") or ""
        batch_dir = parser.batch_dir(source_key, batch_id)
        manifest = self._read_json_file(batch_dir / "manifest.json")
        artifacts = (manifest or {}).get("artifacts", {})
        parsed_path = batch_dir / "parsed.jsonl"
        error_path = batch_dir / "parse_errors.jsonl"

        if manifest is None or not parsed_path.exists():
            missing = []
            if manifest is None:
                missing.append("manifest.json")
            if not parsed_path.exists():
                missing.append("parsed.jsonl")
            return (
                self.VERDICT_MISSING,
                f"当前批次 {batch_id or '未知'} 的解析产物缺失（{('、'.join(missing))}），无法据以完成诊断",
                {
                    "readable": False,
                    "manifest": manifest,
                    "missing_artifacts": missing,
                    "retry": self._retry_basis(source_key, current, "当前批次产物缺失", artifacts),
                },
            )

        status = manifest.get("status")
        if status in self.PENDING_BATCH_STATUSES:
            return (
                self.VERDICT_PENDING,
                f"当前批次 {batch_id} 解析状态为「{status}」，零行/错误数尚未得到确认",
                {
                    "readable": True,
                    "manifest": manifest,
                    "retry": self._retry_basis(
                        source_key, current, manifest.get("failure_reason") or f"解析状态为{status}", artifacts
                    ),
                },
            )

        if int(manifest.get("input_lines", 0)) <= 0:
            return (
                self.VERDICT_CONFIRMED_EMPTY,
                f"当前批次 {batch_id} 已解析完成且输入为 0 行，判定为空批次（确认零行）",
                {"readable": True, "manifest": manifest, "retry": None},
            )

        if int(manifest.get("errors", 0)) > 0:
            return (
                self.VERDICT_HAS_ERRORS,
                f"当前批次 {batch_id} 有 {manifest.get('errors')} 行解析失败",
                {"readable": True, "manifest": manifest, "retry": None},
            )

        return (
            self.VERDICT_COMPLETED,
            f"当前批次 {batch_id} 解析完成且无解析错误",
            {"readable": True, "manifest": manifest, "retry": None},
        )

    def _retry_basis(
        self,
        source_key: str,
        current: dict[str, Any] | None,
        reason: str,
        artifacts: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """失败/未完成时给出明确的重试依据：从哪份输入、哪一行继续。"""
        collect_cp = self._read_checkpoint(source_key) or {}
        return {
            "source_key": source_key,
            "source_batch_id": (current or {}).get("batch_id"),
            "reason": reason,
            "input_file": (artifacts or {}).get("input"),
            "input_lines": (current or {}).get("input_lines", 0),
            "collector_offset": collect_cp.get("offset", 0),
            "collector_line_number": collect_cp.get("line_number", 0),
            "retryable": True,
        }

    def _error_evidence(self, error_records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """本批错误原行：定位到行号和原始日志文本。"""
        evidence = []
        for rec in error_records:
            evidence.append({
                "line_no": rec.get("line_no"),
                "raw": rec.get("raw"),
                "parse_error": rec.get("parse_error"),
                "parser_rule": rec.get("parser_rule"),
            })
        return evidence

    def _history_without_current(
        self,
        parser: LogParserService,
        source_key: str,
        current_batch_id: str | None,
    ) -> dict[str, Any]:
        batches = []
        for manifest in parser.list_batches(source_key):
            if manifest.get("batch_id") == current_batch_id:
                continue
            error_count = int(manifest.get("errors", 0))
            entry = {
                "batch_id": manifest.get("batch_id"),
                "status": manifest.get("status"),
                "input_lines": manifest.get("input_lines", 0),
                "parsed": manifest.get("parsed", 0),
                "errors": error_count,
                "completed_at": manifest.get("completed_at"),
                "input_file": manifest.get("artifacts", {}).get("input"),
                "error_file": manifest.get("artifacts", {}).get("parse_errors"),
            }
            if error_count:
                error_records = self._read_jsonl(
                    parser.batch_dir(source_key, str(manifest.get("batch_id"))) / "parse_errors.jsonl"
                )
                entry["error_lines"] = self._error_evidence(error_records)
            batches.append(entry)

        return {
            "batches": batches,
            "batches_with_errors": sum(1 for b in batches if b["errors"] > 0),
            "excluded_from_current": True,
        }

    # -- 报告发布 --

    def _publish_report_row(self, report: dict[str, Any]) -> None:
        """把当前报告同步到诊断报告列表，供页面查询/复查。"""
        rows = store.rows(self.MODULE)
        values = {
            "报告编号": report["report_id"],
            "关联源": report["source_key"],
            "解析成功率": self._rate_text(report.get("parse_success_rate")),
            "字段完整性": f"{report['field_completeness'].get('overall', 100)}%",
            "延迟分布": self._latency_summary(report["latency_distribution"]),
            "脏数据类型": self._dirty_summary(report["dirty_data_types"]),
            "诊断时间": time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(report["diagnosed_at"])),
            "报告状态": "已生成" if report["completed"] else "诊断中",
            "批次号": report.get("batch_id"),
            "判定": report["verdict"],
            "输入行数": report["input_lines"],
            "解析行数": report["parsed_ok"],
            "错误行数": report["parse_errors"],
            "当前报告文件": report.get("current_report_file"),
            "status": "已生成" if report["completed"] else "诊断中",
            "pending": not report["completed"],
            "abnormal": report["verdict"] in (self.VERDICT_HAS_ERRORS, self.VERDICT_PENDING, self.VERDICT_MISSING),
        }

        for row in rows:
            if row.get("关联源") == report["source_key"] and str(row.get("报告编号", "")).startswith("QL_"):
                row.update(values)
                return
        row = {"id": max((int(r.get("id", 0)) for r in rows), default=0) + 1}
        row.update(values)
        rows.append(row)

    @staticmethod
    def _rate_text(rate: Any) -> str:
        """未完成口径的成功率不写成一个像“0% 解析”的完成数据。"""
        return "—（待核）" if rate is None else f"{rate}%"

    @staticmethod
    def _latency_summary(dist: dict[str, Any]) -> str:
        if not dist or not dist.get("count"):
            return "无延迟数据"
        return f"avg={dist.get('avg')}ms p95={dist.get('p95')}ms"

    @staticmethod
    def _dirty_summary(dirty: dict[str, Any]) -> str:
        if not dirty or not dirty.get("total"):
            return "无"
        return "、".join(f"{k}:{v}" for k, v in dirty.get("categories", {}).items()) or "无"

    @staticmethod
    def _archive_view(report: dict[str, Any]) -> dict[str, Any]:
        view = dict(report)
        view["current_batch"] = {
            k: v for k, v in report.get("current_batch", {}).items() if k != "error_lines"
        }
        view["history"] = {
            "batches_with_errors": report.get("history", {}).get("batches_with_errors", 0),
            "excluded_from_current": True,
        }
        return view

    @staticmethod
    def _relative_path(path: Any) -> str:
        from app.services.base import DATA_ROOT
        try:
            return str(path.relative_to(DATA_ROOT))
        except (ValueError, AttributeError):
            return str(path)

    # -- 当前报告读取 --

    def get_current_report(self, source_key: str) -> dict[str, Any] | None:
        """读取最近一次发布的质量报告（不做重新诊断）。"""
        return self._read_json_file(self._data_dir("exports") / f"quality_current_{source_key}.json")

    # -- 指标计算 --

    def _compute_completeness(self, records: list[dict[str, Any]]) -> dict[str, Any]:
        if not records:
            return {"rate": 100, "total": 0, "description": "无记录"}

        field_counts = defaultdict(lambda: {"present": 0, "total": 0})
        for rec in records:
            fields = rec.get("fields", rec)
            for k, v in fields.items():
                field_counts[k]["total"] += 1
                if v is not None and v != "":
                    field_counts[k]["present"] += 1

        result = {"fields": {}}
        for field, counts in field_counts.items():
            rate = round(counts["present"] / max(counts["total"], 1) * 100, 2)
            result["fields"][field] = rate

        rates = [v for v in result["fields"].values()]
        result["overall"] = round(sum(rates) / max(len(rates), 1), 2) if rates else 100
        return result

    def _compute_latency_distribution(self, records: list[dict[str, Any]]) -> dict[str, Any]:
        latencies = []
        for rec in records:
            fields = rec.get("fields", rec)
            for k in ("latency_ms", "latency", "duration_ms"):
                val = fields.get(k)
                if val is not None:
                    try:
                        latencies.append(float(val))
                    except (ValueError, TypeError):
                        pass

        if not latencies:
            return {"count": 0, "description": "无延迟数据"}

        return {
            "count": len(latencies),
            "min": round(min(latencies), 2),
            "max": round(max(latencies), 2),
            "avg": round(statistics.mean(latencies), 2),
            "p50": round(statistics.median(latencies), 2),
            "p95": round(self._percentile(latencies, 95), 2),
            "p99": round(self._percentile(latencies, 99), 2),
        }

    def _percentile(self, data: list[float], percentile: float) -> float:
        if not data:
            return 0
        sorted_data = sorted(data)
        index = (percentile / 100.0) * (len(sorted_data) - 1)
        lower = int(math.floor(index))
        upper = int(math.ceil(index))
        if lower == upper:
            return sorted_data[lower]
        weight = index - lower
        return sorted_data[lower] * (1 - weight) + sorted_data[upper] * weight

    def _classify_dirty_data(self, error_records: list[dict[str, Any]]) -> dict[str, Any]:
        types = defaultdict(int)
        for rec in error_records:
            error_msg = rec.get("parse_error", "unknown")
            if "正则" in str(error_msg):
                types["regex_mismatch"] += 1
            elif "格式" in str(error_msg):
                types["format_error"] += 1
            elif "字段" in str(error_msg):
                types["missing_field"] += 1
            else:
                types["other"] += 1
        return {"total": len(error_records), "categories": dict(types)}

    def compare_quality_trend(self, source_key: str) -> dict[str, Any]:
        """对比最近两次质量诊断的趋势（仅比较已完成口径的报告）。"""
        reports = self._read_jsonl(self._data_dir("exports") / "quality_reports.jsonl")
        source_reports = [
            r for r in reports
            if r.get("source_key") == source_key and r.get("completed", True)
            and r.get("parse_success_rate") is not None
        ]

        if len(source_reports) < 2:
            return {"source_key": source_key, "status": "需要至少两次诊断才能对比趋势"}

        latest = source_reports[-1]
        previous = source_reports[-2]

        return {
            "source_key": source_key,
            "latest_success_rate": latest.get("parse_success_rate", 0),
            "previous_success_rate": previous.get("parse_success_rate", 0),
            "trend": "up" if latest.get("parse_success_rate", 0) > previous.get("parse_success_rate", 0) else "down" if latest.get("parse_success_rate", 0) < previous.get("parse_success_rate", 0) else "stable",
            "delta": round(latest.get("parse_success_rate", 0) - previous.get("parse_success_rate", 0), 2),
        }
