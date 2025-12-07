"""
Automation helper for driving a local web workflow with Selenium while monitoring
an auxiliary Python program. The script reads an Excel file for field values,
submits them to the website, waits for either a UI change or a completion marker
in the worker program's logs (defaulting to "Report written to outputs/"), and
then finishes the workflow by clicking a series of buttons. You can also set a
working directory for the worker process when it lives in another folder.

Example usage (single command):
    python automation.py --excel-path ./inputs.xlsx --first-column "name" --second-column "email"

Extended usage:
    python automation.py \
        --worker "python other_script.py" \
        --completion-marker "PROCESS COMPLETE" \
        --worker-cwd "C:/Users/User/python_code/new_researcher/gpt-researcher" \
        --excel-path ./inputs.xlsx \
        --first-column "name" \
        --second-column "email" \
        --status-column "Status" \
        --status-value "Done" \
        --output-excel ./inputs_with_status.xlsx \
        --completion-text "Finished" \
        --completion-selector "#status" \
        --first-field "#first-input" \
        --second-field "#second-input" \
        --submit-field "#second-input" \
        --final-buttons "#confirm,#done"
"""

import argparse
import contextlib
import threading
from pathlib import Path
import subprocess

import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait


def launch_worker(
    command: str, completion_marker: str | None, log_path: Path | None, cwd: Path | None = None
):
    """Start the worker process and stream its output.

    Args:
        command: The shell command to run for the worker program.
        completion_marker: Marker string that indicates completion when found in stdout.
        log_path: Optional path to write the streamed logs.

    Returns:
        A tuple of (Popen, completion_event, streaming_thread).
    """

    process = subprocess.Popen(
        command,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        cwd=str(cwd) if cwd else None,
    )

    completion_event = threading.Event()

    def _stream_output():
        with contextlib.ExitStack() as stack:
            log_file = stack.enter_context(log_path.open("w")) if log_path else None
            assert process.stdout is not None
            for line in process.stdout:
                print(line, end="")
                if log_file:
                    log_file.write(line)
                if completion_marker and completion_marker in line:
                    completion_event.set()
            process.stdout.close()

    thread = threading.Thread(target=_stream_output, daemon=True)
    thread.start()
    return process, completion_event, thread


def build_driver(headless: bool) -> webdriver.Chrome:
    """Create a Chrome WebDriver instance."""
    options = Options()
    options.add_argument("--window-size=1400,900")
    if headless:
        options.add_argument("--headless=new")
    service = Service()
    return webdriver.Chrome(service=service, options=options)


def read_excel_rows(excel_path: Path, first_column: str, second_column: str):
    """Yield (index, first_value, second_value) from the specified Excel columns."""
    df = pd.read_excel(excel_path)
    for idx, row in df.iterrows():
        first_value = row[first_column]
        second_value = row[second_column]
        yield idx, "" if pd.isna(first_value) else str(first_value), "" if pd.isna(second_value) else str(second_value)


def write_status_updates(
    excel_path: Path,
    processed_indices: list,
    status_column: str,
    status_value: str,
    output_excel: Path | None = None,
):
    """Mark processed rows in the Excel file with a status value."""

    if not processed_indices:
        return

    df = pd.read_excel(excel_path)
    if status_column not in df.columns:
        df[status_column] = ""

    df.loc[processed_indices, status_column] = status_value

    target_path = output_excel if output_excel else excel_path
    df.to_excel(target_path, index=False)


def fill_fields(driver: webdriver.Chrome, first_locator, second_locator, submit_locator, first_value: str, second_value: str):
    """Populate form fields and submit."""
    wait = WebDriverWait(driver, 10)

    first_input = wait.until(EC.element_to_be_clickable(first_locator))
    first_input.clear()
    first_input.send_keys(first_value)

    second_input = wait.until(EC.element_to_be_clickable(second_locator))
    second_input.clear()
    second_input.send_keys(second_value)

    submit_element = wait.until(EC.element_to_be_clickable(submit_locator))
    submit_element.send_keys(Keys.ENTER)


def wait_for_completion(driver: webdriver.Chrome, condition, process_event: threading.Event, timeout: int):
    """Wait until either the DOM condition is met or the worker signals completion."""

    def _either(driver_obj):
        if process_event.is_set():
            return "process"
        result = condition(driver_obj)
        if result:
            return "dom"
        return False

    return WebDriverWait(driver, timeout, poll_frequency=1).until(_either)


def finish_workflow(driver: webdriver.Chrome, button_locators: list[tuple[str, str]]):
    """Click additional buttons to complete the workflow."""
    wait = WebDriverWait(driver, 10)
    for locator in button_locators:
        button = wait.until(EC.element_to_be_clickable(locator))
        button.click()


def css_locator(selector: str):
    return (By.CSS_SELECTOR, selector)


def parse_button_selectors(selector_string: str) -> list[tuple[str, str]]:
    if not selector_string:
        return []
    selectors = [selector.strip() for selector in selector_string.split(",") if selector.strip()]
    return [css_locator(selector) for selector in selectors]


def parse_args():
    parser = argparse.ArgumentParser(description="Automate a web workflow while monitoring another Python program.")
    parser.add_argument(
        "--worker",
        required=False,
        default="python -m uvicorn main:app --reload",
        help=(
            "Command to run the worker Python program (default matches the provided uvicorn invocation)."
        ),
    )
    parser.add_argument(
        "--worker-cwd",
        type=Path,
        default=None,
        help=(
            "Optional working directory to execute the worker command from. Useful when the server lives "
            "in a different folder (e.g., C:/Users/User/python_code/new_researcher/gpt-researcher)."
        ),
    )
    parser.add_argument(
        "--completion-marker",
        required=False,
        default="Report written to outputs/",
        help="Text that signals completion in worker output.",
    )
    parser.add_argument("--log-path", type=Path, default=None, help="File path to write worker logs.")
    parser.add_argument("--excel-path", type=Path, required=True, help="Path to the Excel file with input data.")
    parser.add_argument("--first-column", required=True, help="Excel column for the first field value.")
    parser.add_argument("--second-column", required=True, help="Excel column for the second field value.")
    parser.add_argument(
        "--status-column",
        default="Status",
        help="Excel column to mark once processing is confirmed.",
    )
    parser.add_argument(
        "--status-value",
        default="Done",
        help="Value to write in the status column when processing finishes.",
    )
    parser.add_argument(
        "--output-excel",
        type=Path,
        default=None,
        help="Optional output Excel path. Defaults to updating the original file in-place.",
    )
    parser.add_argument("--url", default="http://127.0.0.1:8000/#", help="URL to open in the browser.")
    parser.add_argument("--first-field", default="[data-testid='first-field']", help="CSS selector for the first input field.")
    parser.add_argument("--second-field", default="[data-testid='second-field']", help="CSS selector for the second input field.")
    parser.add_argument(
        "--submit-field",
        default="[data-testid='second-field']",
        help="CSS selector for the field to submit (Enter key).",
    )
    parser.add_argument(
        "--completion-selector",
        default="[data-testid='status']",
        help="CSS selector that indicates completion on the page.",
    )
    parser.add_argument(
        "--completion-text",
        default="",
        help="Text to look for in the completion element (optional).",
    )
    parser.add_argument(
        "--final-buttons",
        default="",
        help="Comma-separated CSS selectors for buttons to click after completion.",
    )
    parser.add_argument("--timeout", type=int, default=120, help="Seconds to wait for completion before failing.")
    parser.add_argument("--headless", action="store_true", help="Run Chrome in headless mode.")
    return parser.parse_args()


def main():
    args = parse_args()

    log_path = args.log_path if args.log_path else None
    process, completion_event, stream_thread = launch_worker(
        args.worker, args.completion_marker, log_path, args.worker_cwd
    )

    driver = build_driver(args.headless)
    driver.get(args.url)

    first_locator = css_locator(args.first_field)
    second_locator = css_locator(args.second_field)
    submit_locator = css_locator(args.submit_field)

    completion_locator = css_locator(args.completion_selector)
    completion_condition = EC.visibility_of_element_located(completion_locator)
    if args.completion_text:
        completion_condition = EC.text_to_be_present_in_element(completion_locator, args.completion_text)

    button_locators = parse_button_selectors(args.final_buttons)

    processed_indices: list[int] = []

    try:
        for idx, first_value, second_value in read_excel_rows(
            args.excel_path, args.first_column, args.second_column
        ):
            fill_fields(driver, first_locator, second_locator, submit_locator, first_value, second_value)
            processed_indices.append(idx)

        wait_for_completion(driver, completion_condition, completion_event, args.timeout)
        finish_workflow(driver, button_locators)
        write_status_updates(
            args.excel_path,
            processed_indices,
            args.status_column,
            args.status_value,
            args.output_excel,
        )
    finally:
        process.terminate()
        with contextlib.suppress(subprocess.TimeoutExpired):
            process.wait(timeout=5)
        driver.quit()
        stream_thread.join(timeout=1)


if __name__ == "__main__":
    main()