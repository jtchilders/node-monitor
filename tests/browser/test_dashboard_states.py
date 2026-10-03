"""Task 7 browser-state acceptance tests (RED first, GREEN after assets)."""
from playwright.sync_api import Page, expect


def test_dashboard_states(page: Page):
    page.goto("http://localhost:8765/")
    expect(page.locator('[data-testid="connectivity-status"]')).to_contain_text("Loading")
    expect(page.locator('[data-testid="counter-card"]')).to_be_visible()
    for r in ["1h","3h","6h","12h","24h"]:
        expect(page.locator(f'[data-testid="range-btn"][data-range="{r}"]')).to_be_visible()
    expect(page.locator('[data-testid="data-age"]')).to_be_visible()
    # Narrow responsive layout does not hide metadata
    page.set_viewport_size({"width": 400, "height": 800})
    expect(page.locator('[data-testid="cpu-busy"]')).to_be_visible()
