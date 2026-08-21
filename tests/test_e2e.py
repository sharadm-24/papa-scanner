"""Playwright E2E suite — requires server at BASE_URL with YF_FIXTURE_MODE=1."""
from __future__ import annotations

import os
from pathlib import Path

import pytest
from playwright.sync_api import Page, expect

BASE_URL = os.environ.get("BASE_URL", "http://127.0.0.1:8000")
FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture(autouse=True)
def fail_on_dialog(page: Page):
    dialogs = []

    def _on_dialog(dialog):
        dialogs.append(dialog.message)
        dialog.dismiss()

    page.on("dialog", _on_dialog)
    yield
    assert not dialogs, f"Unexpected browser dialog(s): {dialogs}"


def test_e2e_home_01(page: Page):
    page.goto(BASE_URL + "/")
    expect(page.get_by_text("SSAM Infotech").first).to_be_visible()
    expect(page.get_by_text("Stock Backtest Scanner")).to_be_visible()
    expect(page.get_by_text("Index Monitor").first).to_be_visible()
    page.get_by_role("link", name="Open Scanner").click()
    expect(page).to_have_url(f"{BASE_URL}/backtest")


def test_e2e_home_02_ticker(page: Page):
    page.goto(BASE_URL + "/")
    page.wait_for_function(
        "() => !document.getElementById('ticker-container').innerText.includes('Loading Live Market Data')"
    )
    text = page.locator("#ticker-container").inner_text()
    assert "NIFTY" in text or "VIX" in text


def test_e2e_bt_01_valid_upload(page: Page):
    page.goto(BASE_URL + "/backtest")
    page.set_input_files("#file-input", str(FIXTURES / "valid_signals.csv"))
    expect(page.locator("#dashboard-section")).to_be_visible(timeout=30000)
    expect(page.locator("#results-body tr").first).to_be_visible()
    assert int(page.locator("#stat-total").inner_text()) >= 1


def test_e2e_bt_02_filter(page: Page):
    page.goto(BASE_URL + "/backtest")
    page.set_input_files("#file-input", str(FIXTURES / "valid_signals.csv"))
    expect(page.locator("#dashboard-section")).to_be_visible(timeout=30000)
    page.fill("#filter-symbol", "RELIANCE")
    rows = page.locator("#results-body tr")
    expect(rows.first).to_contain_text("RELIANCE")


def test_e2e_bt_03_bad_csv_no_alert(page: Page):
    page.goto(BASE_URL + "/backtest")
    page.set_input_files("#file-input", str(FIXTURES / "bad_headers.csv"))
    expect(page.locator("#error-banner")).to_be_visible(timeout=15000)
    expect(page.locator("#upload-card")).to_be_visible()
    expect(page.locator("#dashboard-section")).to_be_hidden()


def test_e2e_days_01(page: Page):
    page.goto(BASE_URL + "/days")
    page.fill("#days-input", "5")
    page.set_input_files("#file-input", str(FIXTURES / "valid_signals.csv"))
    expect(page.locator("#dashboard-section")).to_be_visible(timeout=30000)
    expect(page.locator("#results-body")).to_contain_text("Next")


def test_e2e_days_02_invalid_days(page: Page):
    page.goto(BASE_URL + "/days")
    page.fill("#days-input", "0")
    page.set_input_files("#file-input", str(FIXTURES / "valid_signals.csv"))
    expect(page.locator("#error-banner")).to_be_visible(timeout=10000)
    expect(page.locator("#error-banner")).to_contain_text("1 and 60")


def test_e2e_idx_01_compare(page: Page):
    page.goto(BASE_URL + "/index_scanner")
    page.click("#scan-btn")
    expect(page.locator("#results-container")).to_be_visible(timeout=60000)
    expect(page.locator("#table-body tr").first).to_be_visible()
    expect(page.locator("#scan-btn")).to_be_enabled()


def test_e2e_idx_02_query_params(page: Page):
    page.goto(BASE_URL + "/index_scanner")
    page.select_option("#duration-select", "6")
    page.click("#btn-weekly")
    with page.expect_request(lambda r: "months=6" in r.url and "interval=1wk" in r.url) as req:
        page.click("#scan-btn")
    assert req.value


def test_e2e_nav_01(page: Page):
    page.goto(BASE_URL + "/")
    page.locator("nav.topbar .md\\:flex a[href='/backtest']").click()
    expect(page).to_have_url(f"{BASE_URL}/backtest")
    page.locator("a.mode-tab[href='/days']").click()
    expect(page).to_have_url(f"{BASE_URL}/days")
    page.locator("nav.topbar .md\\:flex a[href='/index_scanner']").click()
    expect(page).to_have_url(f"{BASE_URL}/index_scanner")
    page.locator("nav.topbar .md\\:flex a[href='/']").click()
    expect(page).to_have_url(f"{BASE_URL}/")


def test_e2e_mobile_nav(page: Page):
    page.set_viewport_size({"width": 390, "height": 844})
    page.goto(BASE_URL + "/backtest")
    expect(page.locator("#mobile-nav-toggle")).to_be_visible()
    page.click("#mobile-nav-toggle")
    expect(page.locator("#mobile-nav-panel")).to_be_visible()
    page.click("#mobile-nav-panel a[href='/index_scanner']")
    expect(page).to_have_url(f"{BASE_URL}/index_scanner")


def test_e2e_export_and_sample(page: Page):
    page.goto(BASE_URL + "/backtest")
    expect(page.get_by_text("Download Sample CSV")).to_be_visible()
    page.click("#use-sample-btn")
    expect(page.locator("#dashboard-section")).to_be_visible(timeout=30000)
    page.evaluate(
        """() => {
          window.__exportOk = false;
          const orig = PapaUI.exportCsv.bind(PapaUI);
          PapaUI.exportCsv = (rows, name) => {
            window.__exportOk = !!orig(rows, name);
            return window.__exportOk;
          };
        }"""
    )
    page.click("#export-csv-btn")
    assert page.evaluate("() => window.__exportOk") is True
