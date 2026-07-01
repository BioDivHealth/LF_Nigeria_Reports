import csv
import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def load_stage_module(filename, module_name):
    module_path = ROOT / "src" / filename
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class FakeResult(list):
    def fetchall(self):
        return list(self)


class FakeSession:
    def __init__(self, responses):
        self.responses = responses
        self.executed = []
        self.committed = False
        self.rolled_back = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, stmt, params=None):
        sql = str(stmt)
        self.executed.append((sql, params))
        for marker, rows in self.responses:
            if marker in sql:
                return FakeResult(rows)
        return FakeResult([])

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True

    def params_for_sql_containing(self, marker):
        return [params for sql, params in self.executed if marker in sql]


class FakeConnection:
    def __init__(self, responses):
        self.responses = responses
        self.executed = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, stmt, params=None):
        sql = str(stmt)
        self.executed.append((sql, params))
        for marker, rows in self.responses:
            if marker in sql:
                return FakeResult(rows)
        return FakeResult([])


class FakeEngine:
    def __init__(self, responses):
        self.responses = responses

    def connect(self):
        return FakeConnection(self.responses)


def write_csv(path, confirmed="3"):
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = [
        {
            "Week": "1",
            "Year": "2026",
            "States": "Ondo",
            "Suspected": "51",
            "Confirmed": confirmed,
            "Probable": "",
            "HCW": "",
            "Deaths": "0",
        },
        {
            "Week": "1",
            "Year": "2026",
            "States": "Total",
            "Suspected": "51",
            "Confirmed": "3",
            "Probable": "",
            "HCW": "",
            "Deaths": "0",
        },
    ]
    with path.open("w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


class StatusSyncGateTests(unittest.TestCase):
    def test_03a_does_not_mark_enhanced_from_png_only_b2_evidence(self):
        module = load_stage_module("03a_SyncEnhancement.py", "sync_enhancement_gate")
        session = FakeSession(
            [
                ("WHERE enhanced = 'Y'", []),
                ("WHERE (enhanced = 'N'", [("report-1", "Nigeria_01_Jan_26_W1.pdf", "26")]),
            ]
        )

        with patch.object(module, "Session", lambda engine: session):
            with patch.object(module, "record_review_needed") as review_mock:
                module.sync_enhanced_status(
                    object(),
                    {"Lines_Nigeria_01_Jan_26_W1_page3.png"},
                    set(),
                )

        self.assertEqual([], session.params_for_sql_containing("SET enhanced = 'Y'"))
        self.assertEqual([], session.params_for_sql_containing("SET enhanced_name"))
        review_mock.assert_called_once()
        self.assertEqual("block_enhanced_status", review_mock.call_args.kwargs["action"])

    def test_03a_records_review_needed_when_layout_qa_fails(self):
        module = load_stage_module("03a_SyncEnhancement.py", "sync_enhancement_review_needed")
        session = FakeSession(
            [
                ("WHERE enhanced = 'Y'", [("report-1", "Lines_Nigeria_01_Jan_26_W1_page3.png", "Nigeria_01_Jan_26_W1.pdf", "26")]),
                ("WHERE (enhanced = 'N'", []),
            ]
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            module.ENHANCED_FOLDER = Path(temp_dir)
            layout_qa_path = (
                module.ENHANCED_FOLDER
                / "PDFs_Lines_26"
                / "Lines_Nigeria_01_Jan_26_W1_page3.layout_qa.json"
            )
            layout_qa_path.parent.mkdir(parents=True)
            layout_qa_path.write_text(
                json.dumps({"status": "fail", "confidence": "none", "selected_page_index": None}),
                encoding="utf-8",
            )

            with patch.object(module, "Session", lambda engine: session), \
                patch.object(module, "record_review_needed") as review_mock:
                module.sync_enhanced_status(
                    object(),
                    {"Lines_Nigeria_01_Jan_26_W1_page3.png"},
                    {"Lines_Nigeria_01_Jan_26_W1_page3.layout_qa.json"},
                )

        self.assertEqual([{"ids_list": ["report-1"]}], session.params_for_sql_containing("SET enhanced = 'N'"))
        review_mock.assert_called_once()
        self.assertEqual("layout_qa", review_mock.call_args.kwargs["check_type"])
        self.assertEqual("demote_enhanced", review_mock.call_args.kwargs["action"])

    def test_04a_does_not_mark_processed_from_csv_only_b2_evidence(self):
        module = load_stage_module("04a_SyncProcessed.py", "sync_processed_csv_only_gate")
        csv_name = "Lines_Nigeria_01_Jan_26_W1_page3.csv"
        session = FakeSession(
            [
                ("WHERE processed = 'Y'", []),
                ("WHERE (processed IS NULL", [("report-1", "Nigeria_01_Jan_26_W1.pdf", "Lines_Nigeria_01_Jan_26_W1_page3.png", "26", "1")]),
            ]
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            module.CSV_BASE_FOLDER = Path(temp_dir)
            write_csv(module.CSV_BASE_FOLDER / "CSV_LF_26_Sorted" / csv_name)

            with patch.object(module, "Session", lambda engine: session), \
                patch.object(module, "record_review_needed") as review_mock:
                module.sync_processed_status(object(), {csv_name}, set())

        self.assertEqual([], session.params_for_sql_containing("SET processed = 'Y'"))
        review_mock.assert_called_once()
        self.assertEqual("block_processed_status", review_mock.call_args.kwargs["action"])

    def test_04a_does_not_download_historical_processed_csv_for_demote_check(self):
        module = load_stage_module("04a_SyncProcessed.py", "sync_processed_existing_no_bulk_download")
        csv_name = "Lines_Nigeria_01_Jan_26_W1_page3.csv"
        session = FakeSession(
            [
                ("WHERE processed = 'Y'", [("report-1", "Nigeria_01_Jan_26_W1.pdf", "Lines_Nigeria_01_Jan_26_W1_page3.png", "26", "1")]),
                ("WHERE (processed IS NULL", []),
            ]
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            module.CSV_BASE_FOLDER = Path(temp_dir)
            with patch.object(module, "Session", lambda engine: session), \
                patch.object(module, "download_file") as download_mock, \
                patch.object(module, "record_review_needed") as review_mock:
                module.sync_processed_status(object(), {csv_name}, set())

        download_mock.assert_not_called()
        review_mock.assert_not_called()
        self.assertEqual([], session.params_for_sql_containing("SET processed = 'N'"))
        self.assertEqual([], session.params_for_sql_containing("SET processed = 'Y'"))

    def test_04a_marks_processed_after_csv_and_extraction_qa_pass(self):
        module = load_stage_module("04a_SyncProcessed.py", "sync_processed_qa_gate")
        csv_name = "Lines_Nigeria_01_Jan_26_W1_page3.csv"
        qa_name = "Lines_Nigeria_01_Jan_26_W1_page3.extraction_qa.json"
        session = FakeSession(
            [
                ("WHERE processed = 'Y'", []),
                ("WHERE (processed IS NULL", [("report-1", "Nigeria_01_Jan_26_W1.pdf", "Lines_Nigeria_01_Jan_26_W1_page3.png", "26", "1")]),
            ]
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            module.CSV_BASE_FOLDER = Path(temp_dir)
            csv_path = module.CSV_BASE_FOLDER / "CSV_LF_26_Sorted" / csv_name
            write_csv(csv_path)
            csv_path.with_suffix(".extraction_qa.json").write_text(
                json.dumps(
                    {
                        "status": "pass",
                        "validation": {"status": "pass"},
                        "csv_qa": {"status": "pass"},
                    }
                ),
                encoding="utf-8",
            )

            with patch.object(module, "Session", lambda engine: session):
                module.sync_processed_status(object(), {csv_name}, {qa_name})

        update_params = session.params_for_sql_containing("SET processed = 'Y'")
        self.assertEqual([{"ids_list": ["report-1"]}], update_params)

    def test_04a_records_review_needed_when_extraction_qa_fails(self):
        module = load_stage_module("04a_SyncProcessed.py", "sync_processed_review_needed")
        csv_name = "Lines_Nigeria_01_Jan_26_W1_page3.csv"
        qa_name = "Lines_Nigeria_01_Jan_26_W1_page3.extraction_qa.json"
        session = FakeSession(
            [
                ("WHERE processed = 'Y'", []),
                ("WHERE (processed IS NULL", [("report-1", "Nigeria_01_Jan_26_W1.pdf", "Lines_Nigeria_01_Jan_26_W1_page3.png", "26", "1")]),
            ]
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            module.CSV_BASE_FOLDER = Path(temp_dir)
            csv_path = module.CSV_BASE_FOLDER / "CSV_LF_26_Sorted" / csv_name
            write_csv(csv_path)
            csv_path.with_suffix(".extraction_qa.json").write_text(
                json.dumps({"status": "fail", "validation": {"status": "fail"}}),
                encoding="utf-8",
            )

            with patch.object(module, "Session", lambda engine: session), \
                patch.object(module, "record_review_needed") as review_mock:
                module.sync_processed_status(object(), {csv_name}, {qa_name})

        self.assertEqual([], session.params_for_sql_containing("SET processed = 'Y'"))
        review_mock.assert_called_once()
        self.assertEqual("block_processed_status", review_mock.call_args.kwargs["action"])
        self.assertEqual("extraction_qa", review_mock.call_args.kwargs["check_type"])

    def test_05a_excludes_invalid_csvs_from_combine_candidates(self):
        module = load_stage_module("05a_SyncCombiningStatus.py", "sync_combining_qa_gate")
        csv_name = "Lines_Nigeria_01_Jan_26_W1_page3.csv"

        with tempfile.TemporaryDirectory() as temp_dir:
            csv_path = Path(temp_dir) / csv_name
            write_csv(csv_path, confirmed="not a number")

            with patch.object(module, "record_review_needed") as review_mock:
                result = module.get_csvs_to_combine(
                    {csv_name: ("report-1", "26", "1", "N")},
                    {csv_name: csv_path},
                )

        self.assertEqual({}, result)
        review_mock.assert_called_once()
        self.assertEqual("skip_combine_candidate", review_mock.call_args.kwargs["action"])

    def test_05c_refuses_combined_advancement_when_csv_qa_fails(self):
        module = load_stage_module("05c_CombinedStatus.py", "combined_status_qa_gate")
        csv_name = "Lines_Nigeria_01_Jan_26_W1_page3.csv"
        session = FakeSession(
            [
                ("SELECT id::text", [("report-1", "Nigeria_01_Jan_26_W1.pdf", "Lines_Nigeria_01_Jan_26_W1_page3.png", "26", "1", "N")]),
            ]
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            csv_path = Path(temp_dir) / csv_name
            write_csv(csv_path, confirmed="not a number")

            with patch.object(module, "Session", lambda engine: session), \
                patch.object(module, "find_local_csv_files", return_value={csv_name: csv_path}), \
                patch.object(module, "get_existing_records", return_value={"report-1"}), \
                patch.object(module, "update_combined_status") as update_mock, \
                patch.object(module, "record_review_needed") as review_mock:
                result = module.sync_combining_status(object())

        self.assertEqual([], result)
        update_mock.assert_not_called()
        review_mock.assert_called_once()
        self.assertEqual("block_combined_status", review_mock.call_args.kwargs["action"])

    def test_05b_records_review_needed_when_db_push_csv_qa_fails(self):
        module = load_stage_module("05b_PushToDB.py", "push_to_db_review_needed")
        csv_name = "Lines_Nigeria_01_Jan_26_W1_page3.csv"
        engine = FakeEngine(
            [
                (
                    "FROM website_data",
                    [("report-1", "Nigeria_01_Jan_26_W1.pdf", "Lines_Nigeria_01_Jan_26_W1_page3.png", "26", "1", "N")],
                ),
                ("FROM lassa_data", []),
            ]
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            module.BASE_DIR = Path(temp_dir)
            write_csv(Path(temp_dir) / "data" / "processed" / "CSV" / "CSV_LF_26_Sorted" / csv_name, confirmed="bad")

            with patch.object(module, "record_review_needed") as review_mock, \
                patch.object(module, "load_and_normalize_csv") as load_mock:
                affected_rows = module.push_lassa_data_individually(engine)

        self.assertEqual(0, affected_rows)
        load_mock.assert_not_called()
        review_mock.assert_called_once()
        self.assertEqual("skip_db_push", review_mock.call_args.kwargs["action"])

    def test_05b_skips_already_combined_csv_when_db_rows_exist(self):
        module = load_stage_module("05b_PushToDB.py", "push_to_db_skip_combined")
        csv_name = "Lines_Nigeria_01_Jan_26_W1_page3.csv"
        engine = FakeEngine(
            [
                (
                    "FROM website_data",
                    [("report-1", "Nigeria_01_Jan_26_W1.pdf", "Lines_Nigeria_01_Jan_26_W1_page3.png", "26", "1", "Y")],
                ),
                ("FROM lassa_data", [("report-1",)]),
            ]
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            module.BASE_DIR = Path(temp_dir)
            write_csv(Path(temp_dir) / "data" / "processed" / "CSV" / "CSV_LF_26_Sorted" / csv_name)

            with patch.object(module, "load_and_normalize_csv") as load_mock, \
                patch.object(module, "push_data_with_upsert") as push_mock:
                affected_rows = module.push_lassa_data_individually(engine)

        self.assertEqual(0, affected_rows)
        load_mock.assert_not_called()
        push_mock.assert_not_called()


if __name__ == "__main__":
    unittest.main()
