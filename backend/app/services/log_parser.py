"""日志解析：按规则解析原始日志行，提取结构化字段。

一次解析对应一个批次（batch）。每批产物写在独立目录里，并登记“分批台账”和
“当前批次”指针，质量诊断只依据当前批次，不会把上一批的错误当成这一批的错误。
批次一旦被后续批次顶替，只能作为历史证据查阅（含错误原行），不再参与当前判定。
"""
from __future__ import annotations

import json
import re
import time
import uuid
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class LogParserService(BaseModuleService):
    MODULE = "log_parser"
    ENTITY = "解析规则"
    OPERATION_TERM = "解析"
    LIST_FIELDS = ["规则编号", "关联源", "正则表达式", "字段提取", "时间格式", "日志级别", "多行合并", "规则版本"]
    REQUIRED_FIELDS = ["规则编号", "关联源", "正则表达式"]
    STATUS_ORDER = ["草稿", "待校验", "已生效", "已停用"]
    ACTION_RULES = {"提交校验": "待校验", "标记生效": "已生效", "停用规则": "已停用"}
    NEGATIVE_ACTIONS = ["停用规则"]

    # -- 批次产物路径约定 --

    def _batch_root(self, source_key: str) -> Any:
        return self._data_dir("logs") / "batches" / source_key

    def batch_dir(self, source_key: str, batch_id: str) -> Any:
        return self._batch_root(source_key) / batch_id

    def _batch_manifest_file(self, source_key: str) -> Any:
        return self._batch_root(source_key) / "batches.jsonl"

    def _current_batch_file(self, source_key: str) -> Any:
        return self._batch_root(source_key) / "current_batch.json"

    def _legacy_parsed_file(self, source_key: str) -> Any:
        return self._data_dir("logs") / f"{source_key}_parsed.jsonl"

    def _legacy_error_file(self, source_key: str) -> Any:
        return self._data_dir("logs") / f"{source_key}_parse_errors.jsonl"

    # -- 解析入口 --

    def parse_log_file(self, source_key: str) -> dict[str, Any]:
        """解析指定日志源已采集的日志文件，产出一个可复查的当前批次。

        输入文件不存在或为 0 行时同样生成批次，并由质量诊断判为“确认零行”；
        这与产物缺失（产物读不到）是两种不同情况。
        """
        log_file = self._data_dir("logs") / f"{source_key}.jsonl"
        input_present = log_file.exists()

        parsers = [p for p in store.rows(self.MODULE) if p.get("status") == "已生效"]
        records = self._read_jsonl(log_file) if input_present else []

        parsed = []
        errors = []
        for rec in records:
            parsed_entry = self._apply_parsers(rec, parsers)
            if parsed_entry.get("parse_error"):
                errors.append(parsed_entry)
            else:
                parsed.append(parsed_entry)

        batch_id = self._new_batch_id(source_key)
        started_at = time.time()
        bdir = self.batch_dir(source_key, batch_id)
        self._replace_jsonl(bdir / "parsed.jsonl", parsed)
        self._replace_jsonl(bdir / "parse_errors.jsonl", errors)

        # 是否还有本批输入之外的上游明细，供诊断区分“空数据”和“缺产物”
        manifest = {
            "batch_id": batch_id,
            "source_key": source_key,
            "input_lines": len(records),
            "input_signature": self._input_signature(log_file),
            "parsed": len(parsed),
            "errors": len(errors),
            "started_at": started_at,
            "completed_at": time.time(),
            "artifacts": {
                "input": str(log_file) if input_present else None,
                "parsed": str(bdir / "parsed.jsonl"),
                "parse_errors": str(bdir / "parse_errors.jsonl"),
            },
            "status": "已完成",
        }
        self._write_json_file(bdir / "manifest.json", manifest)
        self._write_jsonl(self._batch_manifest_file(source_key), [manifest])

        # 本批产物同时落到兼容路径，下游（映射/输出/聚合）仍按原文件名读取；
        # 这里使用覆盖写，避免历史解析行混进当前批次。
        self._replace_jsonl(self._legacy_parsed_file(source_key), parsed)
        self._replace_jsonl(self._legacy_error_file(source_key), errors)

        self._set_current_batch(source_key, manifest, parsed, errors)

        self._write_checkpoint(f"{source_key}_parse", {
            "batch_id": batch_id,
            "total": len(records),
            "parsed": len(parsed),
            "errors": len(errors),
            "completed_at": manifest["completed_at"],
        })

        return {
            "source_key": source_key,
            "batch_id": batch_id,
            "total": len(records),
            "parsed": len(parsed),
            "errors": len(errors),
            "output": str(bdir / "parsed.jsonl"),
            "status": manifest["status"],
        }

    def _new_batch_id(self, source_key: str) -> str:
        # 同一秒内连续解析也要得到不同批次：带上短随机后缀。
        return f"{source_key}-{time.strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:6]}"

    def _input_signature(self, log_file: Any) -> str | None:
        """本批输入的内容摘要，用于复查报告与输入是否一致。"""
        if not log_file.exists():
            return None
        return self._compute_checksum(log_file.read_text(encoding="utf-8"))

    def begin_batch(self, source_key: str) -> dict[str, Any]:
        """登记一个“进行中”的解析批次（解析过程被中断时留痕）。"""
        log_file = self._data_dir("logs") / f"{source_key}.jsonl"
        batch_id = self._new_batch_id(source_key)
        bdir = self.batch_dir(source_key, batch_id)
        manifest = {
            "batch_id": batch_id,
            "source_key": source_key,
            "input_lines": len(self._read_jsonl(log_file)) if log_file.exists() else 0,
            "input_signature": self._input_signature(log_file),
            "parsed": 0,
            "errors": 0,
            "started_at": time.time(),
            "completed_at": None,
            "artifacts": {
                "input": str(log_file),
                "parsed": str(bdir / "parsed.jsonl"),
                "parse_errors": str(bdir / "parse_errors.jsonl"),
            },
            "status": "进行中",
        }
        # 先落空产物文件：解析中断时这些文件可能只写了一部分，
        # 诊断能据 manifest 与产物内容区分“待核”和“缺产物”。
        self._replace_jsonl(bdir / "parsed.jsonl", [])
        self._replace_jsonl(bdir / "parse_errors.jsonl", [])
        self._write_json_file(bdir / "manifest.json", manifest)
        self._write_jsonl(self._batch_manifest_file(source_key), [manifest])
        previous = self.get_current_batch(source_key) or {}
        previous_id = previous.get("batch_id")
        self._set_current_batch(source_key, manifest, [], [], previous_batch_id=previous_id)
        return manifest

    def mark_batch_interrupted(self, source_key: str, batch_id: str, reason: str = "解析进程中断") -> dict[str, Any]:
        """标记批次中断：保留本批输入与错误原行，供诊断判为“待核”并给出重试依据。"""
        manifest = self._read_json_file(self.batch_dir(source_key, batch_id) / "manifest.json")
        if manifest is None:
            return {"error": f"批次 {batch_id} 不存在"}
        manifest["status"] = "已中断"
        manifest["failure_reason"] = reason
        manifest["interrupted_at"] = time.time()
        self._write_json_file(self.batch_dir(source_key, batch_id) / "manifest.json", manifest)
        self._rewrite_batch_ledger(source_key, manifest)
        current = self.get_current_batch(source_key)
        if (current or {}).get("batch_id") == batch_id:
            self._set_current_batch(
                source_key,
                manifest,
                (current or {}).get("evidence", {}).get("parsed_records", []),
                (current or {}).get("evidence", {}).get("error_records", []),
                previous_batch_id=(current or {}).get("previous_batch_id"),
            )
        return manifest

    def _rewrite_batch_ledger(self, source_key: str, manifest: dict[str, Any]) -> None:
        manifest_file = self._batch_manifest_file(source_key)
        ledgers = self._read_jsonl(manifest_file)
        updated = [manifest if item.get("batch_id") == manifest.get("batch_id") else item for item in ledgers]
        self._replace_jsonl(manifest_file, updated)

    def _set_current_batch(
        self,
        source_key: str,
        manifest: dict[str, Any],
        parsed: list[dict[str, Any]],
        errors: list[dict[str, Any]],
        previous_batch_id: str | None = None,
    ) -> None:
        if previous_batch_id is None:
            existing = self.get_current_batch(source_key) or {}
            previous_batch_id = existing.get("batch_id")
            if previous_batch_id == manifest.get("batch_id"):
                previous_batch_id = existing.get("previous_batch_id")
        pointer = dict(manifest)
        pointer["previous_batch_id"] = previous_batch_id
        pointer["evidence"] = {
            "parsed_records": parsed,
            "error_records": errors,
        }
        self._write_json_file(self._current_batch_file(source_key), pointer)

    # -- 批次查询 --

    def get_current_batch(self, source_key: str) -> dict[str, Any] | None:
        """读取当前批次指针（含本批输入统计、产物路径和证据行）。"""
        return self._read_json_file(self._current_batch_file(source_key))

    def list_batches(self, source_key: str) -> list[dict[str, Any]]:
        """按时间倒序列出该源的全部解析批次（历史批次仍可查错误原行）。"""
        manifests = self._read_jsonl(self._batch_manifest_file(source_key))
        return list(reversed(manifests))

    def get_batch(self, source_key: str, batch_id: str) -> dict[str, Any] | None:
        """按批次号读取历史批次证据，供值班员排查旧异常。"""
        manifest = self._read_json_file(self.batch_dir(source_key, batch_id) / "manifest.json")
        if manifest is None:
            return None
        manifest = dict(manifest)
        manifest["parsed_records"] = self._read_jsonl(self.batch_dir(source_key, batch_id) / "parsed.jsonl")
        manifest["error_records"] = self._read_jsonl(self.batch_dir(source_key, batch_id) / "parse_errors.jsonl")
        manifest["is_current"] = (self.get_current_batch(source_key) or {}).get("batch_id") == batch_id
        return manifest

    # -- 解析实现 --

    def _apply_parsers(self, rec: dict[str, Any], parsers: list[dict[str, Any]]) -> dict[str, Any]:
        raw = rec.get("raw", "")
        result = dict(rec)
        result["fields"] = {}
        result["parse_error"] = None

        if not parsers:
            # 无规则时使用默认解析
            m = re.match(r"(\S+) (\S+) \[(\w+)\] (\S+) (\S+) (.+)", raw)
            if m:
                result["fields"] = {
                    "timestamp": m.group(1),
                    "host": m.group(2),
                    "source": m.group(3),
                    "level": m.group(4),
                    "module": m.group(5),
                    "message": m.group(6),
                }
            else:
                result["parse_error"] = "默认正则未匹配"
            return result

        for parser in parsers:
            try:
                pattern = parser.get("正则表达式", "")
                if pattern and pattern not in ("", "日志解析样例1"):
                    m = re.search(pattern, raw)
                    if m:
                        result["fields"] = m.groupdict() if m.groupdict() else {str(i): g for i, g in enumerate(m.groups())}
                        result["parser_rule"] = parser["规则编号"]
                        return result
            except re.error:
                continue

        # 默认解析兜底
        result["fields"]["raw_message"] = raw
        result["fields"]["collected_at"] = rec.get("collected_at")
        return result

    def match_source_to_parser(self, source_id: int) -> list[dict[str, Any]]:
        """查找匹配指定日志源的解析规则。"""
        source_code = f"LOG_-{source_id:04d}"
        matches = []
        for p in store.rows(self.MODULE):
            if p.get("status") == "已生效" and (p.get("关联源") == source_code or p.get("关联源") == str(source_id)):
                matches.append(p)
        return matches
