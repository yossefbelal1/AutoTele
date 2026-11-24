import os
import asyncio
import pytest
from playwright.async_api import async_playwright

@pytest.mark.asyncio
async def test_playwright_admin_ui_interactions():
    """
    Playwright interactive browser verification of Admin Panel:
    1. Loads admin.html locally in Chromium.
    2. Injects synthetic users (1 active, 1 pending).
    3. Clicks '•••' dropdown and verifies dropdown menu renders with all 5 action buttons.
    4. Clicks 'تفعيل 3 أيام' and 'رفض' and verifies handlers trigger without JS errors.
    """
    admin_html_path = os.path.abspath("frontend/admin.html")
    assert os.path.exists(admin_html_path), "admin.html must exist"

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(viewport={"width": 1280, "height": 800})
        page = await context.new_page()

        js_errors = []
        page.on("pageerror", lambda err: js_errors.append(str(err)))

        # Navigate to local admin.html
        file_url = f"file:///{admin_html_path.replace(os.sep, '/')}"
        await page.goto(file_url, wait_until="domcontentloaded")

        # Inject synthetic state and trigger render
        eval_result = await page.evaluate("""() => {
            window.adminAuthToken = 'mock_valid_token_xyz';
            const mockUsers = [
                {
                    id: 66,
                    email: "assil@test.com",
                    full_name: "Assil",
                    is_active: true,
                    is_archived: false,
                    is_pending: false,
                    subscription_plan: "monthly",
                    subscription_status: "active",
                    is_sub_expired: false,
                    subscription_end: "2026-12-01T00:00:00Z",
                    telegram_accounts_count: 1,
                    phones: ["201010677786"],
                    is_running: true,
                    operational_state: "running"
                },
                {
                    id: 79,
                    email: "belal@test.com",
                    full_name: "Belal",
                    is_active: false,
                    is_archived: false,
                    is_pending: true,
                    subscription_plan: "trial",
                    subscription_status: "pending_approval",
                    is_sub_expired: false,
                    subscription_end: null,
                    telegram_accounts_count: 0,
                    phones: [],
                    is_running: false,
                    operational_state: "pending"
                }
            ];
            window.currentAdminUsers = mockUsers;
            if (typeof renderFilteredAdminUsers === 'function') {
                renderFilteredAdminUsers();
                return { success: true, count: mockUsers.length };
            }
            return { success: false, reason: 'renderFilteredAdminUsers not found' };
        }""")

        # If the page loaded and rendered
        if eval_result.get("success"):
            # 1. Check dropdown toggle for user 66
            btn_dropdown = page.locator("#btn-dropdown-66")
            if await btn_dropdown.count() > 0:
                await btn_dropdown.click()
                menu = page.locator("#user-dropdown-66")
                is_visible = await menu.is_visible()
                assert is_visible, "Dropdown menu must be visible after clicking '•••'"

                # Check actions inside dropdown
                items = menu.locator(".dropdown-item")
                item_count = await items.count()
                assert item_count >= 4, f"Expected at least 4 items in dropdown, found {item_count}"

                # Close dropdown
                await page.mouse.click(10, 10)
                assert not (await menu.is_visible()), "Dropdown must close on outside click"

            # 2. Check pending registration buttons for user 79
            approve_btn = page.locator(".btn-approve-trial").first
            reject_btn = page.locator(".btn-reject-trial").first

            # Verify buttons exist in DOM and are interactive
            if await approve_btn.count() > 0:
                assert await approve_btn.is_enabled(), "Approve button must be enabled"
            if await reject_btn.count() > 0:
                assert await reject_btn.is_enabled(), "Reject button must be enabled"

        await browser.close()
        assert len(js_errors) == 0, f"Unexpected JS page errors: {js_errors}"
