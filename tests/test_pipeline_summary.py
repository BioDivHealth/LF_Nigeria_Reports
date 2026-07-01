import tempfile
import unittest
from pathlib import Path

from main import (
    PipelineStepSummary,
    collect_qa_artifact_counts,
    count_csv_data_rows,
    format_pipeline_summary_markdown,
    format_pipeline_summary_text,
    write_github_step_summary,
)


class PipelineSummaryTests(unittest.TestCase):
    def test_formats_successful_and_failed_step_rows(self):
        steps = [
            PipelineStepSummary(1, 2, "URL_Sourcing", "success", 1.234),
            PipelineStepSummary(2, 2, "ExportData", "failed", 0.5, "boom | pipe"),
        ]

        with tempfile.TemporaryDirectory() as temp_dir:
            text = format_pipeline_summary_text(
                steps,
                pipeline_success=False,
                completed_steps=1,
                total_steps=2,
                total_runtime_seconds=1.734,
                base_dir=Path(temp_dir),
            )
            markdown = format_pipeline_summary_markdown(
                steps,
                pipeline_success=False,
                completed_steps=1,
                total_steps=2,
                total_runtime_seconds=1.734,
                base_dir=Path(temp_dir),
            )

        self.assertIn("Overall status: completed with errors", text)
        self.assertIn("Steps completed: 1/2", text)
        self.assertIn("Review needed: 0", text)
        self.assertIn("| Review needed | 0 |", markdown)
        self.assertIn("1/2 URL_Sourcing: success in 1.23s", text)
        self.assertIn("2/2 ExportData: failed in 0.50s - boom | pipe", text)
        self.assertIn("| 2/2 ExportData | failed | 0.50s | boom \\| pipe |", markdown)

    def test_includes_export_row_count_and_qa_counts(self):
        steps = [PipelineStepSummary(1, 1, "ExportData", "success", 2.0)]

        with tempfile.TemporaryDirectory() as temp_dir:
            base_dir = Path(temp_dir)
            exports_dir = base_dir / "exports"
            exports_dir.mkdir()
            (exports_dir / "lassa_data_latest.csv").write_text(
                "year,week,states\n2026,1,Ondo\n2026,1,Total\n",
                encoding="utf-8",
            )
            processed_dir = base_dir / "data" / "processed" / "CSV"
            processed_dir.mkdir(parents=True)
            (processed_dir / "sample.layout_qa.json").write_text("{}", encoding="utf-8")
            (processed_dir / "sample.extraction_qa.json").write_text("{}", encoding="utf-8")
            (processed_dir / "differing_outputs.txt").write_text("x", encoding="utf-8")
            (base_dir / "data" / "processed" / "review_needed.jsonl").write_text(
                '{"stage":"SyncProcessed","check_type":"csv_qa"}\n'
                '{"stage":"CombinedStatus","check_type":"extraction_qa"}\n',
                encoding="utf-8",
            )

            self.assertEqual(2, count_csv_data_rows(exports_dir / "lassa_data_latest.csv"))
            self.assertEqual(
                {"layout_qa": 1, "extraction_qa": 1, "differing_outputs": 1},
                collect_qa_artifact_counts(base_dir),
            )
            text = format_pipeline_summary_text(
                steps,
                pipeline_success=True,
                completed_steps=1,
                total_steps=1,
                total_runtime_seconds=2.0,
                base_dir=base_dir,
            )

        self.assertIn("Latest export rows: 2", text)
        self.assertIn("layout_qa=1, extraction_qa=1, differing_outputs=1", text)
        self.assertIn("Review needed: 2", text)

    def test_writes_markdown_to_fake_github_summary_path(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            summary_path = Path(temp_dir) / "summary.md"
            wrote = write_github_step_summary(
                "## Summary\n\nBody",
                env={"GITHUB_STEP_SUMMARY": str(summary_path)},
            )

            self.assertTrue(wrote)
            self.assertEqual("## Summary\n\nBody\n", summary_path.read_text(encoding="utf-8"))

    def test_github_summary_writer_noops_when_unset(self):
        self.assertFalse(write_github_step_summary("## Summary", env={}))


if __name__ == "__main__":
    unittest.main()
