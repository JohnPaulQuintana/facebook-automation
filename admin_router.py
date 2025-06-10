import re
from playwright.sync_api import Playwright, sync_playwright, expect


def run(playwright: Playwright) -> None:
    browser = playwright.chromium.launch(headless=False)
    context = browser.new_context()
    page = context.new_page()
    page.goto("http://192.168.100.1/")
    page.locator("#txt_Username").click()
    page.locator("#txt_Username").fill("root")
    page.locator("#txt_Password").click()
    page.locator("#txt_Password").fill("*Programmer")
    page.get_by_role("button", name="Log In").click()
    page.locator("#menuIframe").content_frame.locator("#wifidevIcon").click()
    page.locator("#menuIframe").content_frame.locator("#ContectdevmngtPageSrc").content_frame.get_by_role("link", name="Refresh").click()
    page.locator("#menuIframe").content_frame.locator("#ContectdevmngtPageSrc").content_frame.locator("#devlist").click()

    # ---------------------
    context.close()
    browser.close()


with sync_playwright() as playwright:
    run(playwright)
