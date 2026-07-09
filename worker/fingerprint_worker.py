from __future__ import annotations

import json
import hashlib
import time
from pathlib import Path
from urllib.parse import urlparse

from loguru import logger

from config.settings import Settings
from downloader.video_downloader import VideoDownloader
from fingerprint.video_probe import get_media_probe
from matcher.dinov2_matcher import DinoV2Config
from matcher.dinov2_matcher import DinoV2Embedder
from matcher.dinov2_matcher import DinoV2EmbeddingIndex
from matcher.dinov2_matcher import load_embedding_index
from matcher.dinov2_matcher import run_dinov2_fingerprint
from matcher.dinov2_matcher import save_embedding_index
from matcher.dinov2_matcher import target_embedding_cache_key
from matcher.duration_gate import should_reject_for_short_duration
from matcher.media_type_gate import should_reject_non_video
from matcher.multi_stage_fingerprint import PipelineResult, StageOutcome
from matcher.candidate_matcher import (
    score_title_token_overlap,
)
from work_queue.crawler_media_queue import CrawlerMediaQueue
from work_queue.models import QueueJob
from storage.file_retention import RejectedFileRetentionPolicy
from storage.processing_metadata_store import ProcessingMetadataStore


class FingerprintWorker:
    """Worker: claim crawler jobs, run staged fingerprinting, and feedback results."""

    def __init__(
        self,
        settings: Settings,
        *,
        enabled_techniques: set[str] | None = None,
        keep_non_matches: bool = False,
        dinov2_target_sample_fps_override: float | None = None,
        dinov2_candidate_sample_fps_override: float | None = None,
        dinov2_cosine_threshold_override: float | None = None,
        dinov2_l2_score_threshold_override: float | None = None,
        dinov2_margin_threshold_override: float | None = None,
        dinov2_min_consecutive_frames_override: int | None = None,
        dinov2_max_target_frame_step_override: int | None = None,
        dinov2_min_run_avg_cosine_override: float | None = None,
    ):
        self.settings = settings
        if settings.queue.backend != "crawler":
            raise ValueError(
                f"Unsupported queue backend: {settings.queue.backend}. Phase A currently supports crawler backend only."
            )

        self.downloader = VideoDownloader(
            download_dir=settings.downloader.download_dir,
            request_timeout_seconds=settings.downloader.request_timeout_seconds,
            enable_tor=settings.downloader.enable_tor,
            tor_proxy_url=settings.downloader.tor_proxy_url,
        )
        self.queue = CrawlerMediaQueue(
            db_path=settings.queue.crawler_media_db_path,
            worker_name=settings.queue.worker_name,
        )
        self.metadata_store = ProcessingMetadataStore(settings.storage_policy.metadata_db_path)
        self.retention_policy = RejectedFileRetentionPolicy(
            self.metadata_store,
            max_rejected_files=settings.storage_policy.max_rejected_files,
            max_rejected_bytes_mb=settings.storage_policy.max_rejected_bytes_mb,
            delete_overflow=settings.storage_policy.delete_rejected_overflow,
        )
        self.enabled_techniques = self._normalize_techniques(enabled_techniques)
        self.keep_non_matches = bool(keep_non_matches)
        self.dinov2_config = DinoV2Config(
            model_name=str(settings.dinov2.model_name),
            target_sample_fps=float(
                dinov2_target_sample_fps_override
                if dinov2_target_sample_fps_override is not None
                else settings.dinov2.target_sample_fps
            ),
            candidate_sample_fps=float(
                dinov2_candidate_sample_fps_override
                if dinov2_candidate_sample_fps_override is not None
                else settings.dinov2.candidate_sample_fps
            ),
            cosine_threshold=float(
                dinov2_cosine_threshold_override
                if dinov2_cosine_threshold_override is not None
                else settings.dinov2.cosine_threshold
            ),
            l2_score_threshold=float(
                dinov2_l2_score_threshold_override
                if dinov2_l2_score_threshold_override is not None
                else settings.dinov2.l2_score_threshold
            ),
            margin_threshold=float(
                dinov2_margin_threshold_override
                if dinov2_margin_threshold_override is not None
                else settings.dinov2.margin_threshold
            ),
            min_consecutive_frames=int(
                dinov2_min_consecutive_frames_override
                if dinov2_min_consecutive_frames_override is not None
                else settings.dinov2.min_consecutive_frames
            ),
            max_target_frame_step=int(
                dinov2_max_target_frame_step_override
                if dinov2_max_target_frame_step_override is not None
                else settings.dinov2.max_target_frame_step
            ),
            min_run_avg_cosine=float(
                dinov2_min_run_avg_cosine_override
                if dinov2_min_run_avg_cosine_override is not None
                else settings.dinov2.min_run_avg_cosine
            ),
        )
        self.dinov2_embedder = DinoV2Embedder(
            model_name=self.dinov2_config.model_name,
            device=settings.dinov2.device,
        )
        self._target_embedding_cache_key: str | None = None
        self._target_embedding_cache: DinoV2EmbeddingIndex | None = None

    @staticmethod
    def _normalize_techniques(enabled_techniques: set[str] | None) -> set[str]:
        if not enabled_techniques:
            return {"dinov2"}
        normalized = {item.strip().lower() for item in enabled_techniques if item and item.strip()}
        unknown = sorted(normalized - {"dinov2"})
        if unknown:
            raise ValueError(f"Unknown techniques: {', '.join(unknown)}")
        if not normalized:
            raise ValueError("At least one fingerprinting technique must be selected")
        return normalized

    @staticmethod
    def _synthetic_asset_id(key: str) -> int:
        digest = hashlib.sha1(key.encode("utf-8")).hexdigest()
        return 2_000_000_000 + int(digest[:8], 16)

    @staticmethod
    def _is_video_candidate(path: Path) -> bool:
        name = str(path.name).split("?", 1)[0]
        suffix = Path(name).suffix.lower()
        return suffix in {".mp4", ".mkv", ".avi", ".mov", ".webm", ".m4v"}

    @staticmethod
    def _queue_task_key(asset_id: int) -> str:
        return f"queue_asset:{asset_id}"

    @staticmethod
    def _file_task_key(candidate_path: str, target_path: str, techniques: set[str]) -> str:
        normalized_path = str(Path(candidate_path).resolve())
        normalized_target = str(Path(target_path).resolve()) if target_path else ""
        technique_key = ",".join(sorted(techniques))
        return f"file:{normalized_path}|target:{normalized_target}|techniques:{technique_key}"

    @staticmethod
    def _build_filename(job: QueueJob) -> str:
        parsed_path = urlparse(job.media_url).path
        suffix = Path(parsed_path).suffix or ".bin"
        return f"asset_{job.asset_id}{suffix}"

    def _target_embedding_cache_dir(self) -> Path:
        return Path("storage/target_fingerprints")

    def _target_embedding_cache_file(self, cache_key: str) -> Path:
        return self._target_embedding_cache_dir() / f"{cache_key}.npz"

    def _load_or_build_target_embedding_index(self, target_file: Path) -> DinoV2EmbeddingIndex | None:
        if not target_file or not target_file.exists():
            return None

        cache_key = target_embedding_cache_key(
            target_path=target_file,
            model_name=self.dinov2_config.model_name,
            sample_fps=self.dinov2_config.target_sample_fps,
        )
        if self._target_embedding_cache_key == cache_key and self._target_embedding_cache is not None:
            return self._target_embedding_cache

        cache_dir = self._target_embedding_cache_dir()
        cache_dir.mkdir(parents=True, exist_ok=True)
        cache_file = self._target_embedding_cache_file(cache_key)

        if cache_file.exists():
            try:
                index = load_embedding_index(cache_file)
                if index.embeddings.size > 0:
                    self._target_embedding_cache_key = cache_key
                    self._target_embedding_cache = index
                    return index
            except Exception:
                logger.warning("Failed to load DINOv2 target embedding cache at {}", cache_file)

        logger.info("Building DINOv2 target embedding cache for {}", target_file)
        index = self.dinov2_embedder.embed_video(
            str(target_file),
            sample_fps=self.dinov2_config.target_sample_fps,
        )
        save_embedding_index(index, cache_file)

        meta_file = cache_file.with_suffix(".meta.json")
        meta_payload = {
            "target_path": str(target_file.expanduser().resolve()),
            "model_name": self.dinov2_config.model_name,
            "target_sample_fps": float(self.dinov2_config.target_sample_fps),
            "embedding_count": int(index.embeddings.shape[0]),
        }
        meta_file.write_text(json.dumps(meta_payload, sort_keys=True), encoding="utf-8")

        self._target_embedding_cache_key = cache_key
        self._target_embedding_cache = index
        return index

    def _resolve_target_file(self) -> Path:
        configured = (self.settings.pipeline.target_file_path or "").strip()
        if configured:
            p = Path(configured)
            if not p.is_absolute():
                p = Path.cwd() / p
            return p

        target_dir = Path("target")
        if not target_dir.exists():
            return Path("")

        candidates = sorted(
            [
                p
                for p in target_dir.iterdir()
                if p.is_file() and p.suffix.lower() in {".mp4", ".mkv", ".avi", ".mov", ".webm", ".m4v"}
            ]
        )
        return candidates[0] if candidates else Path("")

    def _infer_source_domain(self, job: QueueJob) -> str | None:
        if job.source_domain:
            return job.source_domain.strip().lower() or None
        host = (urlparse(job.media_url).hostname or "").strip().lower()
        return host or None

    def requeue_unfinished_claims(self) -> int:
        return self.queue.requeue_stale_claimed_jobs(
            stale_after_seconds=self.settings.queue.reclaim_claimed_after_seconds
        )

    def compare_task_status_counts(self) -> dict[str, int]:
        return self.metadata_store.compare_task_status_counts()

    def reset_processing_state(self) -> None:
        self.metadata_store.reset_all_tables()

    def clear_downloaded_assets(self) -> tuple[int, int, int]:
        """Delete files in download_dir and mark matching processed records as deleted.

        Returns: (files_deleted, bytes_deleted, metadata_rows_marked_deleted)
        """

        download_dir = Path(self.settings.downloader.download_dir).expanduser().resolve()
        if not download_dir.exists() or not download_dir.is_dir():
            return 0, 0, 0

        deleted_paths: list[str] = []
        files_deleted = 0
        bytes_deleted = 0
        for entry in sorted(download_dir.iterdir()):
            if not entry.is_file():
                continue
            size = entry.stat().st_size
            entry.unlink()
            files_deleted += 1
            bytes_deleted += size
            deleted_paths.append(str(entry))

        marked = self.metadata_store.mark_assets_deleted_by_local_paths(
            local_paths=deleted_paths,
            note="deleted by --clear-assets",
        )
        return files_deleted, bytes_deleted, marked

    def _run_pipeline_for_candidate(
        self,
        *,
        target_title: str,
        candidate_url: str,
        candidate_path: str,
        target_file: Path,
    ) -> PipelineResult:
        if target_file and target_file.exists():
            target_index = self._load_or_build_target_embedding_index(target_file)
            if target_index is None:
                return PipelineResult(
                    final_status="failed",
                    piracy_score=0.0,
                    outcomes=[
                        StageOutcome(
                            stage_name="stage0_sanitization",
                            score=0.0,
                            decision="reject",
                            note="target video missing or unreadable",
                            details={"target_path": str(target_file)},
                        )
                    ],
                )

            return run_dinov2_fingerprint(
                target_title=target_title,
                candidate_url=candidate_url,
                candidate_path=candidate_path,
                target_index=target_index,
                embedder=self.dinov2_embedder,
                config=self.dinov2_config,
                low_threshold=self.settings.pipeline.phase_b_low_match_threshold,
                high_threshold=self.settings.pipeline.phase_b_high_match_threshold,
            )

        # Fallback if no target media exists: keep legacy title-token stage score.
        stage_2 = score_title_token_overlap(
            target_title=target_title,
            candidate_text=f"{candidate_url} {Path(candidate_path).name}",
        )
        score = float(stage_2["score"])
        if score >= self.settings.pipeline.phase_b_high_match_threshold:
            status = "matched"
        elif score <= self.settings.pipeline.phase_b_low_match_threshold:
            status = "no_match_pending_review"
        else:
            status = "uncertain_manual_review"

        return PipelineResult(
            final_status=status,
            piracy_score=score,
            outcomes=[
                StageOutcome(
                    stage_name="stage2_visual_quick",
                    score=score,
                    decision=status,
                    note="fallback title-token quick score",
                    details={"matched_tokens": stage_2["matched_tokens"]},
                )
            ],
        )

    def _persist_pipeline_result(
        self,
        *,
        asset_id: int,
        source_domain: str | None,
        target_title: str,
        target_file: Path,
        candidate_url: str,
        candidate_path: str,
        file_size_bytes: int | None,
        duration_seconds: float | None,
        pipeline_result: PipelineResult,
        task_key: str,
    ) -> tuple[str, int]:
        run_id = self.metadata_store.create_processing_run(
            asset_id=asset_id,
            source_domain=source_domain,
            target_title=target_title,
            target_path=str(target_file) if target_file else "",
            candidate_path=candidate_path,
            final_status=pipeline_result.final_status,
            piracy_score=pipeline_result.piracy_score,
        )

        for stage in pipeline_result.outcomes:
            self.metadata_store.record_stage_result(
                run_id=run_id,
                asset_id=asset_id,
                stage_name=stage.stage_name,
                score=stage.score,
                decision=stage.decision,
                note=stage.note,
                details=stage.details,
            )

        final_status = pipeline_result.final_status

        self.metadata_store.record_asset(
            asset_id=asset_id,
            media_url=candidate_url,
            local_path=candidate_path,
            file_size_bytes=file_size_bytes,
            duration_seconds=duration_seconds,
            decision=final_status,
            note=(
                f"pipeline_score={pipeline_result.piracy_score:.4f}; "
                f"target={target_title}; "
                f"source_domain={source_domain or ''}"
            ),
        )
        self.metadata_store.complete_compare_task(task_key=task_key, run_id=run_id, outcome_status=final_status)
        return final_status, run_id

    def process_job(self, job: QueueJob) -> str:
        logger.info("Processing asset_id={} url={}", job.asset_id, job.media_url)

        target_title = (self.settings.pipeline.target_title or "").strip()
        target_file = self._resolve_target_file()
        source_domain = self._infer_source_domain(job)
        if not target_title:
            target_title = Path(target_file).stem if target_file else "target"
        task_key = self._queue_task_key(job.asset_id)
        self.metadata_store.start_compare_task(
            task_key=task_key,
            mode="queue",
            asset_id=job.asset_id,
            candidate_ref=job.media_url,
            target_path=str(target_file) if target_file else "",
            techniques=list(self.enabled_techniques),
        )

        reject_non_video, non_video_reason = should_reject_non_video(
            job.media_type,
            job.media_url,
            enable_tor=self.settings.downloader.enable_tor,
        )
        if reject_non_video:
            self.queue.update_job_status(job.asset_id, "rejected_non_video", last_error=non_video_reason)
            self.metadata_store.record_asset(
                asset_id=job.asset_id,
                media_url=job.media_url,
                local_path="",
                file_size_bytes=None,
                duration_seconds=None,
                decision="rejected_non_video",
                note=non_video_reason,
            )
            self.metadata_store.complete_compare_task(
                task_key=task_key,
                run_id=None,
                outcome_status="rejected_non_video",
            )
            self.retention_policy.enforce()
            logger.info("Rejected asset_id={} reason={}", job.asset_id, non_video_reason)
            return "rejected_non_video"

        local_path = self.downloader.download(job.media_url, filename=self._build_filename(job))
        file_size_bytes = Path(local_path).stat().st_size if Path(local_path).exists() else None

        probe = get_media_probe(local_path)
        duration_seconds = probe.get("duration_seconds")
        reject, reason = should_reject_for_short_duration(
            duration_seconds=float(duration_seconds) if duration_seconds is not None else None,
            threshold_seconds=self.settings.pipeline.short_video_threshold_seconds,
        )

        if reject:
            self.queue.update_job_status(job.asset_id, "rejected_too_short", last_error=reason)
            self.metadata_store.record_asset(
                asset_id=job.asset_id,
                media_url=job.media_url,
                local_path=local_path,
                file_size_bytes=file_size_bytes,
                duration_seconds=duration_seconds,
                decision="rejected_too_short",
                note=reason,
            )
            self.metadata_store.complete_compare_task(
                task_key=task_key,
                run_id=None,
                outcome_status="rejected_too_short",
            )
            self.retention_policy.enforce()
            logger.info("Rejected asset_id={} reason={}", job.asset_id, reason)
            return "rejected_too_short"

        pipeline_result = self._run_pipeline_for_candidate(
            target_title=target_title,
            candidate_url=job.media_url,
            candidate_path=local_path,
            target_file=target_file,
        )

        final_status = pipeline_result.final_status

        if final_status == "matched":
            self.queue.mark_match(
                job.asset_id,
                matched_title=target_title,
                confidence=pipeline_result.piracy_score,
            )

            if source_domain:
                boosted = self.queue.boost_domain_priority(
                    source_domain,
                    boosted_priority=self.settings.pipeline.pirate_domain_boost_priority,
                )
                self.metadata_store.record_crawler_feedback(
                    asset_id=job.asset_id,
                    source_domain=source_domain,
                    feedback_type="pirate_domain_priority_boost",
                    feedback_value=f"pending_jobs_updated={boosted}",
                )

        else:
            self.queue.update_job_status(job.asset_id, final_status)

        persisted_status, run_id = self._persist_pipeline_result(
            asset_id=job.asset_id,
            source_domain=source_domain,
            target_title=target_title,
            target_file=target_file,
            candidate_url=job.media_url,
            candidate_path=local_path,
            file_size_bytes=file_size_bytes,
            duration_seconds=float(duration_seconds) if duration_seconds is not None else None,
            pipeline_result=pipeline_result,
            task_key=task_key,
        )

        if final_status == "matched":
            self.metadata_store.record_piracy_match(
                asset_id=job.asset_id,
                media_url=job.media_url,
                source_domain=source_domain,
                target_title=target_title,
                candidate_path=local_path,
                piracy_score=pipeline_result.piracy_score,
                status="pirated",
                evidence={
                    "run_id": run_id,
                    "stages": [
                        {
                            "stage_name": s.stage_name,
                            "score": s.score,
                            "decision": s.decision,
                            "note": s.note,
                        }
                        for s in pipeline_result.outcomes
                    ],
                },
            )

        if (not self.keep_non_matches) and persisted_status != "matched":
            deleted_non_matches = self.retention_policy.delete_non_matching_assets()
            if deleted_non_matches:
                logger.info("Deleted {} non-matching files", deleted_non_matches)
        logger.info(
            "Pipeline decision asset_id={} final_status={} score={:.4f}",
            job.asset_id,
            final_status,
            pipeline_result.piracy_score,
        )
        return final_status

    def compare_single_video(self, candidate_ref: str) -> str:
        target_title = (self.settings.pipeline.target_title or "").strip()
        target_file = self._resolve_target_file()
        if not target_title:
            target_title = Path(target_file).stem if target_file else "target"

        parsed = urlparse(candidate_ref)
        if parsed.scheme in {"http", "https", "file"}:
            candidate_path = self.downloader.download(candidate_ref, filename=Path(parsed.path or candidate_ref).name)
            candidate_url = candidate_ref
        elif Path(candidate_ref).exists():
            candidate_path = str(Path(candidate_ref).expanduser().resolve())
            candidate_url = str(Path(candidate_ref).expanduser().resolve())
        else:
            raise FileNotFoundError(f"Candidate video does not exist: {candidate_ref}")

        local_candidate = Path(candidate_path)
        task_key = self._file_task_key(str(local_candidate), str(target_file), self.enabled_techniques)
        self.metadata_store.start_compare_task(
            task_key=task_key,
            mode="single_file",
            candidate_ref=str(local_candidate),
            target_path=str(target_file) if target_file else "",
            techniques=list(self.enabled_techniques),
            asset_id=self._synthetic_asset_id(str(local_candidate)),
        )

        if not self._is_video_candidate(local_candidate):
            self.metadata_store.complete_compare_task(
                task_key=task_key,
                run_id=None,
                outcome_status="rejected_non_video",
            )
            return "rejected_non_video"

        probe = get_media_probe(str(local_candidate))
        duration_seconds = probe.get("duration_seconds")
        asset_id = self._synthetic_asset_id(str(local_candidate))
        file_size_bytes = local_candidate.stat().st_size if local_candidate.exists() else None

        pipeline_result = self._run_pipeline_for_candidate(
            target_title=target_title,
            candidate_url=candidate_url,
            candidate_path=str(local_candidate),
            target_file=target_file,
        )

        status, _ = self._persist_pipeline_result(
            asset_id=asset_id,
            source_domain=(urlparse(candidate_url).hostname or None),
            target_title=target_title,
            target_file=target_file,
            candidate_url=candidate_url,
            candidate_path=str(local_candidate),
            file_size_bytes=file_size_bytes,
            duration_seconds=float(duration_seconds) if duration_seconds is not None else None,
            pipeline_result=pipeline_result,
            task_key=task_key,
        )

        if (not self.keep_non_matches) and status != "matched":
            self.retention_policy.delete_non_matching_assets()
        return status

    def compare_directory(self, directory_path: str, *, max_files: int | None = None) -> dict[str, int]:
        base = Path(directory_path).expanduser().resolve()
        if not base.exists() or not base.is_dir():
            raise NotADirectoryError(f"Directory does not exist: {directory_path}")

        processed = 0
        summary: dict[str, int] = {
            "matched": 0,
            "no_match_pending_review": 0,
            "uncertain_manual_review": 0,
            "failed": 0,
            "rejected_non_video": 0,
            "skipped_completed": 0,
        }
        for file_path in sorted([p for p in base.iterdir() if p.is_file()]):
            if max_files is not None and processed >= max_files:
                break
            if not self._is_video_candidate(file_path):
                continue

            task_key = self._file_task_key(str(file_path), str(self._resolve_target_file()), self.enabled_techniques)
            if self.metadata_store.is_compare_task_completed(task_key=task_key):
                summary["skipped_completed"] += 1
                continue

            try:
                status = self.compare_single_video(str(file_path))
                summary[status] = summary.get(status, 0) + 1
            except Exception as exc:
                logger.exception("Directory compare failed for {}: {}", file_path, exc)
                summary["failed"] += 1
            finally:
                processed += 1

        return summary

    def run_once(self) -> bool:
        job = self.queue.claim_job()
        if job is None:
            return False

        try:
            self.process_job(job)
        except Exception as exc:
            self.metadata_store.fail_compare_task(
                task_key=self._queue_task_key(job.asset_id),
                error_message=str(exc),
            )
            final_status = self.queue.mark_failed_or_retry(
                job.asset_id,
                error_message=str(exc),
                max_retry_count=self.settings.queue.max_retry_count,
            )
            self.metadata_store.record_asset(
                asset_id=job.asset_id,
                media_url=job.media_url,
                local_path="",
                file_size_bytes=None,
                duration_seconds=None,
                decision=final_status,
                note=str(exc),
            )
            logger.exception("Phase A processing failed for asset_id={}: {}", job.asset_id, exc)

        return True

    def run_loop(self, max_jobs: int | None = None) -> None:
        processed = 0
        poll_seconds = max(1, int(self.settings.queue.poll_interval_seconds))

        while True:
            if max_jobs is not None and processed >= max_jobs:
                logger.info("Reached max_jobs={} and stopping", max_jobs)
                break

            worked = self.run_once()
            if worked:
                processed += 1
                continue

            if max_jobs is not None:
                logger.info(
                    "No pending jobs and max_jobs was set (processed={}). Stopping.",
                    processed,
                )
                break

            logger.info("No pending jobs. Sleeping {}s", poll_seconds)
            time.sleep(poll_seconds)

    def close(self) -> None:
        self.queue.close()
        self.metadata_store.close()
