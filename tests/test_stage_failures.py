import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import main


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def load_stage_module(filename, module_name):
    module_path = ROOT / "src" / filename
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class ConnectedEngine:
    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    def connect(self):
        return self.Connection()


class StageFailureTests(unittest.TestCase):
    def test_enhancement_attempts_all_reports_then_raises(self):
        module = load_stage_module("03b_TableEnhancement_Supabase.py", "enhancement_failures")
        reports = [
            {"id": "report-1", "new_name": "Nigeria_13_Jun_26_W24.pdf", "year": 26, "week": 24},
            {"id": "report-2", "new_name": "Nigeria_20_Jun_26_W25.pdf", "year": 26, "week": 25},
        ]
        engine = object()

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            module.RAW_FOLDER = temp_path / "raw"
            module.ENHANCED_FOLDER = temp_path / "enhanced"
            for report in reports:
                pdf_path = module.RAW_FOLDER / str(report["year"]) / report["new_name"]
                pdf_path.parent.mkdir(parents=True, exist_ok=True)
                pdf_path.write_bytes(b"%PDF-1.4\n")

            with patch.object(module, "get_db_engine", return_value=engine), \
                patch.object(module, "get_b2_report_filenames", return_value=set()), \
                patch.object(module, "get_reports_to_enhance", return_value=reports), \
                patch.object(
                    module,
                    "enhance_report_pdf",
                    side_effect=[True, RuntimeError("cannot unpack non-iterable numpy.int32 object")],
                ) as enhance_mock, \
                patch.object(module, "update_enhanced_status") as update_mock, \
                patch.object(module, "record_review_needed") as review_mock:
                with self.assertRaisesRegex(RuntimeError, "failed for 1/2 reports"):
                    module.process_reports_from_supabase()

        self.assertEqual(2, enhance_mock.call_count)
        update_mock.assert_called_once_with(engine, "report-1", "Lines_Nigeria_13_Jun_26_W24_page3.png")
        review_mock.assert_called_once()
        self.assertEqual("table_enhancement", review_mock.call_args.kwargs["check_type"])
        self.assertEqual("block_enhanced_status", review_mock.call_args.kwargs["action"])

    def test_extraction_attempts_all_reports_then_raises(self):
        module = load_stage_module("04b_LLM_Extraction_Supabase.py", "extraction_failures")
        reports = [
            {"id": "report-1", "year": 26, "week": 24, "enhanced_name": "week24.png"},
            {"id": "report-2", "year": 26, "week": 25, "enhanced_name": "week25.png"},
        ]
        module.DATABASE_URL = "postgresql://example"

        with patch.object(module, "get_db_engine", return_value=ConnectedEngine()), \
            patch.object(module, "get_reports_to_process", return_value=reports), \
            patch.object(module, "process_single_report", side_effect=[True, False]) as process_mock:
            with self.assertRaisesRegex(RuntimeError, "failed for 1/2 reports"):
                module.process_reports_from_supabase(model_name="test-model")

        self.assertEqual(2, process_mock.call_count)

    def test_pipeline_skips_export_after_failure_but_runs_cloud_sync(self):
        called_steps = []

        def fake_import(module_name, file_path):
            step_name = Path(file_path).stem

            def run_step():
                called_steps.append(step_name)
                if step_name == "03b_TableEnhancement_Supabase":
                    raise RuntimeError("enhancement failed")

            return SimpleNamespace(main=run_step)

        with patch.object(main, "import_module_from_file", side_effect=fake_import), \
            patch.object(main, "emit_pipeline_summary") as summary_mock:
            success = main.run_pipeline()

        self.assertFalse(success)
        self.assertIn("06_CloudSync", called_steps)
        self.assertNotIn("07_ExportData", called_steps)

        step_summaries = summary_mock.call_args.args[0]
        statuses = {step.name: step.status for step in step_summaries}
        self.assertEqual("failed", statuses["TableEnhancement_Supabase"])
        self.assertEqual("success", statuses["CloudSync"])
        self.assertEqual("skipped", statuses["ExportData"])
        self.assertEqual(10, summary_mock.call_args.args[2])


if __name__ == "__main__":
    unittest.main()
