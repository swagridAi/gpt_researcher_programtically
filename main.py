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
import http.client
import threading
import time
from pathlib import Path
from urllib.parse import urlparse
import subprocess

import pandas as pd
from selenium import webdriver
from selenium.common.exceptions import WebDriverException
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


def _is_service_available(url: str, timeout: int) -> bool:
    """Return True when the URL responds without connection errors."""

    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        return True

    conn_cls = http.client.HTTPSConnection if parsed.scheme == "https" else http.client.HTTPConnection
    port = parsed.port or (443 if parsed.scheme == "https" else 80)

    try:
        conn = conn_cls(parsed.hostname, port, timeout=timeout)
        conn.request("HEAD", parsed.path or "/")
        conn.getresponse()
        return True
    except OSError:
        return False
    finally:
        with contextlib.suppress(Exception):
            conn.close()


def wait_for_service(url: str, timeout: int, interval: float = 1.0) -> bool:
    """Poll a URL until it responds or the timeout elapses."""

    deadline = time.time() + timeout
    while time.time() < deadline:
        if _is_service_available(url, timeout=timeout):
            return True
        time.sleep(interval)
    return False


def _normalize_key(value: str) -> str:
    return value.strip().lower()


def build_link_map(link_excel_path: Path, name_column: str, domain_column: str) -> dict[str, str]:
    """Load link groups into a lookup of normalized name -> domain string."""

    df = pd.read_excel(link_excel_path)
    if name_column not in df.columns or domain_column not in df.columns:
        raise ValueError(
            f"Missing required columns '{name_column}' or '{domain_column}' in {link_excel_path}"
        )

    link_map: dict[str, str] = {}
    for _, row in df.iterrows():
        raw_name = row[name_column]
        if pd.isna(raw_name):
            continue

        normalized_name = _normalize_key(str(raw_name))
        domain_value = row[domain_column]
        link_map[normalized_name] = "" if pd.isna(domain_value) else str(domain_value)
    return link_map


def read_excel_rows(
    excel_path: Path,
    first_column: str,
    second_column: str,
    link_map: dict[str, str] | None = None,
    split_delimiter: str = ",",
):
    """Yield (index, first_value, second_value) from the specified Excel columns.

    When a link map is provided, values in the second column can be comma-separated
    group names. Each group will emit its own tuple with the mapped domain string,
    enabling scenarios like running both consulting and academic domain queries for
    a single topic row.
    """

    df = pd.read_excel(excel_path)
    if first_column not in df.columns or second_column not in df.columns:
        raise ValueError(
            f"Missing required columns '{first_column}' or '{second_column}' in {excel_path}"
        )

    for idx, row in df.iterrows():
        first_value = row[first_column]
        first_text = "" if pd.isna(first_value) else str(first_value)
        raw_second = row[second_column]

        if link_map is None:
            second_text = "" if pd.isna(raw_second) else str(raw_second)
            yield idx, first_text, second_text
            continue

        if pd.isna(raw_second):
            yield idx, first_text, ""
            continue

        groups = [group.strip() for group in str(raw_second).split(split_delimiter) if group.strip()]
        if not groups:
            yield idx, first_text, str(raw_second)
            continue

        for group in groups:
            mapped_value = link_map.get(_normalize_key(group))
            yield idx, first_text, mapped_value if mapped_value is not None else group


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


def wait_for_completion(
    driver: webdriver.Chrome, condition, process_event: threading.Event | None, timeout: int
):
    """Wait until either the DOM condition is met or the worker signals completion."""

    def _either(driver_obj):
        if process_event and process_event.is_set():
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
        default=None,
        help=(
            "Command to run the worker Python program. When omitted, the script will not launch a worker process."
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
        default=None,
        help=(
            "Text that signals completion in worker output. Only used when a worker command is provided."
        ),
    )
    parser.add_argument("--log-path", type=Path, default=None, help="File path to write worker logs.")
    parser.add_argument(
        "--excel-path",
        type=Path,
        default=Path("input/topics.xlsx"),
        help="Path to the Excel file with input data (topics by default).",
    )
    parser.add_argument(
        "--first-column",
        default="Research Topics",
        help="Excel column for the first field value (topic).",
    )
    parser.add_argument(
        "--second-column",
        default="Links",
        help="Excel column for the second field value or link group names.",
    )
    parser.add_argument(
        "--link-excel-path",
        type=Path,
        default=Path("input/links.xlsx"),
        help="Optional Excel path that maps link group names to domain lists.",
    )
    parser.add_argument(
        "--link-name-column",
        default="Link Group Name",
        help="Column in the link map Excel that identifies each group name.",
    )
    parser.add_argument(
        "--link-domain-column",
        default="Domains",
        help="Column in the link map Excel that contains domains for each group.",
    )
    parser.add_argument(
        "--link-delimiter",
        default=",",
        help="Delimiter used to split multiple link group names in the topic sheet.",
    )
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
    parser.add_argument(
        "--service-wait",
        type=int,
        default=40,
        help="Seconds to wait for the target URL to start responding before launching the browser.",
    )
    parser.add_argument("--headless", action="store_true", help="Run Chrome in headless mode.")
    return parser.parse_args()


def main():
    args = parse_args()

    process = None
    completion_event = None
    stream_thread = None
    log_path = args.log_path if args.log_path else None
    if args.worker:
        process, completion_event, stream_thread = launch_worker(
            args.worker, args.completion_marker, log_path, args.worker_cwd
        )

    if not wait_for_service(args.url, args.service_wait):
        if process:
            process.terminate()
            with contextlib.suppress(subprocess.TimeoutExpired):
                process.wait(timeout=5)
        raise RuntimeError(
            "The target service did not respond within the allotted time. "
            "Start the web app (or set --url to a reachable address) before running this script."
        )

    driver = build_driver(args.headless)
    try:
        driver.get(args.url)
    except WebDriverException as exc:
        driver.quit()
        if process:
            process.terminate()
            with contextlib.suppress(subprocess.TimeoutExpired):
                process.wait(timeout=5)
        raise RuntimeError(
            f"Failed to open {args.url}. Ensure the target service is running or provide a reachable --url."
        ) from exc

    first_locator = css_locator(args.first_field)
    second_locator = css_locator(args.second_field)
    submit_locator = css_locator(args.submit_field)

    completion_locator = css_locator(args.completion_selector)
    completion_condition = EC.visibility_of_element_located(completion_locator)
    if args.completion_text:
        completion_condition = EC.text_to_be_present_in_element(completion_locator, args.completion_text)

    button_locators = parse_button_selectors(args.final_buttons)

    link_map = None
    if args.link_excel_path:
        link_map = build_link_map(args.link_excel_path, args.link_name_column, args.link_domain_column)

    processed_indices: set[int] = set()

    try:
        for idx, first_value, second_value in read_excel_rows(
            args.excel_path,
            args.first_column,
            args.second_column,
            link_map=link_map,
            split_delimiter=args.link_delimiter,
        ):
            fill_fields(driver, first_locator, second_locator, submit_locator, first_value, second_value)
            processed_indices.add(idx)

        wait_for_completion(driver, completion_condition, completion_event, args.timeout)
        finish_workflow(driver, button_locators)
        write_status_updates(
            args.excel_path,
            sorted(processed_indices),
            args.status_column,
            args.status_value,
            args.output_excel,
        )
    finally:
        if process:
            process.terminate()
            with contextlib.suppress(subprocess.TimeoutExpired):
                process.wait(timeout=5)
        driver.quit()
        if stream_thread:
            stream_thread.join(timeout=1)


if __name__ == "__main__":
    main()