#!/usr/bin/env python3
"""
main.py: Orchestrates the Lassa Fever Report Processing Pipeline

This script serves as the main entry point for the entire Lassa Fever report processing pipeline.
It sequentially executes each step of the pipeline in the correct order:

1. URL_Sourcing: Scrapes NCDC website for Lassa fever reports and extracts metadata
2. PDF_Download_Supabase: Downloads PDF reports and organizes them by year
3. SyncEnhancement: Syncs enhancement status between local and cloud storage
4. TableEnhancement_Supabase: Processes PDFs to enhance table visibility for extraction
5. SyncProcessed: Syncs processed files status between local and cloud storage
6. LLM_Extraction_Supabase: Extracts table data using AI and sorts it into CSV files
7. SyncCombiningStatus: Syncs combining status between local and cloud storage
8. PushToDB: Pushes extracted data to the database
9. CloudSync: Syncs all data to cloud storage
10. ExportData: Exports data from Supabase to CSV files in the repository

Usage:
    python main.py

This script is designed to be the single entry point for the entire pipeline,
making it easier to containerize and deploy the application.
"""

import sys
import time
import logging
import importlib.util
import os
import csv
from dataclasses import dataclass
from pathlib import Path

# Add the project root directory to Python path to fix import issues
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))


# Import centralized logging configuration
try:
    from utils.logging_config import configure_logging
    from utils.review_needed import summarize_review_needed
except ImportError:
    from src.utils.logging_config import configure_logging
    from src.utils.review_needed import summarize_review_needed

# Configure logging
configure_logging()

# Load environment variables from .env file
def load_env_file(env_path):
    """Load environment variables from .env file"""
    if not env_path.exists():
        logging.warning(f".env file not found at {env_path}")
        return
    
    logging.info(f"Loading environment variables from {env_path}")
    with open(env_path, 'r') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#') or '=' not in line:
                continue
            key, value = line.split('=', 1)
            # Remove quotes if present
            value = value.strip('"\'')
            os.environ[key] = value
            logging.debug(f"Loaded environment variable: {key}")

# Load environment variables
env_path = project_root / '.env'
load_env_file(env_path)

def import_module_from_file(module_name, file_path):
    """
    Import a module from a file path.
    
    Args:
        module_name (str): Name to give the imported module
        file_path (str): Path to the Python file to import
        
    Returns:
        module: The imported module object
    """
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


@dataclass
class PipelineStepSummary:
    step_number: int
    total_steps: int
    name: str
    status: str
    duration_seconds: float
    note: str = ""


def _format_duration(seconds):
    return f"{seconds:.2f}s"


def _short_note(value, max_length=120):
    note = str(value or "").replace("\n", " ").strip()
    if len(note) <= max_length:
        return note
    return f"{note[:max_length - 3]}..."


def count_csv_data_rows(csv_path):
    csv_path = Path(csv_path)
    if not csv_path.exists():
        return None

    with csv_path.open(newline="", encoding="utf-8") as infile:
        reader = csv.reader(infile)
        try:
            next(reader)
        except StopIteration:
            return 0
        return sum(1 for _ in reader)


def collect_qa_artifact_counts(base_dir):
    processed_dir = Path(base_dir) / "data" / "processed"
    counts = {
        "layout_qa": 0,
        "extraction_qa": 0,
        "differing_outputs": 0,
    }
    if not processed_dir.exists():
        return counts

    counts["layout_qa"] = sum(1 for _ in processed_dir.rglob("*.layout_qa.json"))
    counts["extraction_qa"] = sum(1 for _ in processed_dir.rglob("*.extraction_qa.json"))
    counts["differing_outputs"] = sum(1 for _ in processed_dir.rglob("differing_outputs.txt"))
    return counts


def _summary_metrics(pipeline_success, completed_steps, total_steps, total_runtime_seconds, base_dir):
    export_rows = count_csv_data_rows(Path(base_dir) / "exports" / "lassa_data_latest.csv")
    qa_counts = collect_qa_artifact_counts(base_dir)
    review_summary = summarize_review_needed(base_dir=base_dir)
    return {
        "overall_status": "success" if pipeline_success else "completed with errors",
        "completed_steps": completed_steps,
        "total_steps": total_steps,
        "total_runtime": _format_duration(total_runtime_seconds),
        "export_rows": export_rows,
        "qa_counts": qa_counts,
        "review_needed": review_summary,
    }


def format_pipeline_summary_text(step_summaries, pipeline_success, completed_steps, total_steps, total_runtime_seconds, base_dir):
    metrics = _summary_metrics(pipeline_success, completed_steps, total_steps, total_runtime_seconds, base_dir)
    export_rows = metrics["export_rows"] if metrics["export_rows"] is not None else "unavailable"
    qa_counts = metrics["qa_counts"]
    review_needed = metrics["review_needed"]
    lines = [
        "Pipeline run summary",
        f"Overall status: {metrics['overall_status']}",
        f"Steps completed: {completed_steps}/{total_steps}",
        f"Total runtime: {metrics['total_runtime']}",
        f"Latest export rows: {export_rows}",
        (
            "QA artifacts: "
            f"layout_qa={qa_counts['layout_qa']}, "
            f"extraction_qa={qa_counts['extraction_qa']}, "
            f"differing_outputs={qa_counts['differing_outputs']}"
        ),
        f"Review needed: {review_needed['total']}",
        "Step timings:",
    ]

    for step in step_summaries:
        note_suffix = f" - {step.note}" if step.note else ""
        lines.append(
            f"  {step.step_number}/{step.total_steps} {step.name}: "
            f"{step.status} in {_format_duration(step.duration_seconds)}{note_suffix}"
        )

    return "\n".join(lines)


def _markdown_cell(value):
    return str(value).replace("|", "\\|").replace("\n", " ").strip()


def format_pipeline_summary_markdown(step_summaries, pipeline_success, completed_steps, total_steps, total_runtime_seconds, base_dir):
    metrics = _summary_metrics(pipeline_success, completed_steps, total_steps, total_runtime_seconds, base_dir)
    export_rows = metrics["export_rows"] if metrics["export_rows"] is not None else "unavailable"
    qa_counts = metrics["qa_counts"]
    review_needed = metrics["review_needed"]
    qa_summary = (
        f"layout_qa={qa_counts['layout_qa']}, "
        f"extraction_qa={qa_counts['extraction_qa']}, "
        f"differing_outputs={qa_counts['differing_outputs']}"
    )

    lines = [
        "## Lassa Pipeline Run Summary",
        "",
        "| Metric | Value |",
        "| --- | --- |",
        f"| Overall status | {_markdown_cell(metrics['overall_status'])} |",
        f"| Steps completed | {completed_steps}/{total_steps} |",
        f"| Total runtime | {metrics['total_runtime']} |",
        f"| Latest export rows | {export_rows} |",
        f"| QA artifacts | {_markdown_cell(qa_summary)} |",
        f"| Review needed | {review_needed['total']} |",
        "",
        "| Step | Status | Duration | Note |",
        "| --- | --- | --- | --- |",
    ]

    for step in step_summaries:
        lines.append(
            "| "
            f"{step.step_number}/{step.total_steps} {_markdown_cell(step.name)} | "
            f"{_markdown_cell(step.status)} | "
            f"{_format_duration(step.duration_seconds)} | "
            f"{_markdown_cell(step.note)} |"
        )

    return "\n".join(lines)


def write_github_step_summary(markdown, env=None):
    env = env if env is not None else os.environ
    summary_path = env.get("GITHUB_STEP_SUMMARY")
    if not summary_path:
        return False

    try:
        with open(summary_path, "a", encoding="utf-8") as outfile:
            outfile.write(markdown.rstrip())
            outfile.write("\n")
        return True
    except OSError as exc:
        logging.warning(f"Could not write GitHub step summary: {exc}")
        return False


def emit_pipeline_summary(step_summaries, pipeline_success, completed_steps, total_steps, total_runtime_seconds, base_dir):
    try:
        text_summary = format_pipeline_summary_text(
            step_summaries,
            pipeline_success,
            completed_steps,
            total_steps,
            total_runtime_seconds,
            base_dir,
        )
        for line in text_summary.splitlines():
            logging.info(line)

        markdown_summary = format_pipeline_summary_markdown(
            step_summaries,
            pipeline_success,
            completed_steps,
            total_steps,
            total_runtime_seconds,
            base_dir,
        )
        write_github_step_summary(markdown_summary)
    except Exception as exc:
        logging.warning(f"Could not emit pipeline run summary: {exc}", exc_info=True)

def run_pipeline():
    """
    Run the complete Lassa Fever report processing pipeline.
    
    Executes each step of the pipeline in sequence, with appropriate logging.
    Continues to the next step if a script fails due to missing environment variables.
    """
    # Define base directory and script paths
    base_dir = Path(__file__).parent
    scripts = [
        {"name": "URL_Sourcing", "path": base_dir / "src" / "01_URL_Sourcing.py"},
        {"name": "PDF_Download_Supabase", "path": base_dir / "src" / "02_PDF_Download_Supabase.py"},
        {"name": "SyncEnhancement", "path": base_dir / "src" / "03a_SyncEnhancement.py"},
        {"name": "TableEnhancement_Supabase", "path": base_dir / "src" / "03b_TableEnhancement_Supabase.py"},
        {"name": "SyncProcessed", "path": base_dir / "src" / "04a_SyncProcessed.py"},
        {"name": "LLM_Extraction_Supabase", "path": base_dir / "src" / "04b_LLM_Extraction_Supabase.py"},
        {"name": "SyncCombiningStatus", "path": base_dir / "src" / "05a_SyncCombiningStatus.py"},
        {"name": "PushToDB", "path": base_dir / "src" / "05b_PushToDB.py"},
        {"name": "CombinedStatus", "path": base_dir / "src" / "05c_CombinedStatus.py"},
        {"name": "StateCleaning", "path": base_dir / "src" / "05d_CleanStates.py"},
        {"name": "CloudSync", "path": base_dir / "src" / "06_CloudSync.py"},
        {"name": "ExportData", "path": base_dir / "src" / "07_ExportData.py"}
    ]
    
    # Track overall success of the pipeline
    pipeline_success = True
    completed_steps = 0
    step_summaries = []
    pipeline_start_time = time.time()
    total_steps = len(scripts)
    
    # Execute each script in sequence
    for i, script in enumerate(scripts):
        script_name = script["name"]
        script_path = script["path"]
        
        if not script_path.exists():
            logging.error(f"Script not found: {script_path}")
            pipeline_success = False
            step_summaries.append(
                PipelineStepSummary(i + 1, total_steps, script_name, "failed", 0.0, "script not found")
            )
            continue
        
        logging.info(f"Starting step {i+1}/{total_steps}: {script_name}")
        start_time = time.time()
        
        try:
            # Import and run the module
            module = import_module_from_file(f"lassa_{script_name.lower()}", script_path)
            
            # Check if the module has a main function
            if hasattr(module, "main"):
                try:
                    module.main()
                except SystemExit as e:
                    # If the script exits with code 1, log it and continue with the next script
                    # This handles cases where environment variables are missing
                    if e.code == 1:
                        logging.warning(f"{script_name} exited with code 1. This may be due to missing environment variables. Continuing with next step.")
                        pipeline_success = False
                        step_summaries.append(
                            PipelineStepSummary(
                                i + 1,
                                total_steps,
                                script_name,
                                "failed",
                                time.time() - start_time,
                                "exited with code 1",
                            )
                        )
                        continue
                    else:
                        # For other exit codes, re-raise the exception
                        raise
            else:
                logging.warning(f"No main() function found in {script_name}, attempting to execute module directly")
            
            # Special case for URL_Sourcing which has a separate process_file_status_update function
            if script_name == "URL_Sourcing" and hasattr(module, "process_file_status_update"):
                try:
                    logging.info(f"Running process_file_status_update for {script_name}")
                    # Get the engine from the module if it exists
                    if hasattr(module, "engine"):
                        module.process_file_status_update(module.engine)
                    else:
                        logging.warning(f"Skipping process_file_status_update for {script_name}: No engine found in module")
                except SystemExit as e:
                    if e.code == 1:
                        logging.warning(f"process_file_status_update for {script_name} exited with code 1. Continuing with next step.")
                        pipeline_success = False
                        step_summaries.append(
                            PipelineStepSummary(
                                i + 1,
                                total_steps,
                                script_name,
                                "failed",
                                time.time() - start_time,
                                "process_file_status_update exited with code 1",
                            )
                        )
                        continue
                    else:
                        raise
                except Exception as e:
                    logging.error(f"Error in process_file_status_update for {script_name}: {e}")
                    pipeline_success = False
                    step_summaries.append(
                        PipelineStepSummary(
                            i + 1,
                            total_steps,
                            script_name,
                            "failed",
                            time.time() - start_time,
                            _short_note(f"process_file_status_update error: {e}"),
                        )
                    )
                    continue
            
            elapsed_time = time.time() - start_time
            logging.info(f"Completed {script_name} in {elapsed_time:.2f} seconds")
            completed_steps += 1
            step_summaries.append(
                PipelineStepSummary(i + 1, total_steps, script_name, "success", elapsed_time)
            )
            
        except Exception as e:
            logging.error(f"Error executing {script_name}: {e}", exc_info=True)
            pipeline_success = False
            step_summaries.append(
                PipelineStepSummary(
                    i + 1,
                    total_steps,
                    script_name,
                    "failed",
                    time.time() - start_time,
                    _short_note(e),
                )
            )
            # Continue with the next script instead of exiting
            continue
    
    if pipeline_success:
        logging.info("Pipeline completed successfully!")
    else:
        logging.warning(f"Pipeline completed with errors. {completed_steps}/{len(scripts)} steps completed successfully.")

    emit_pipeline_summary(
        step_summaries,
        pipeline_success,
        completed_steps,
        total_steps,
        time.time() - pipeline_start_time,
        base_dir,
    )
    
    return pipeline_success

if __name__ == "__main__":
    logging.info("Starting Lassa Fever report processing pipeline")
    success = run_pipeline()
    logging.info("Pipeline execution finished")
    
    # Exit with appropriate code based on pipeline success
    sys.exit(0 if success else 1)
