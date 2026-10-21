use std::fs;

const SPEC_PATH: &str = "tests/luc654-ui-validation.spec.mjs";
const FIXTURE_SERVER_PATH: &str = "tests/fixtures/luc654-ui-fixture-server.mjs";
const PACKAGE_PATH: &str = "package.json";
const PLAYWRIGHT_CONFIG_PATH: &str = "playwright.config.mjs";

#[test]
fn luc654_playwright_validation_entrypoint_is_checked_in() {
    let spec = fs::read_to_string(SPEC_PATH).expect("read LUC-654 Playwright spec");
    let fixture = fs::read_to_string(FIXTURE_SERVER_PATH).expect("read LUC-654 fixture server");
    let package =
        fs::read_to_string(PACKAGE_PATH).expect("read LUC-654 Playwright package entrypoint");
    let config =
        fs::read_to_string(PLAYWRIGHT_CONFIG_PATH).expect("read LUC-654 Playwright config");

    for required in [
        "1440, height: 900",
        "1024, height: 768",
        "980, height: 720",
        "390, height: 844",
        "'/overview'",
        "'/issues'",
        "IMS card drag/drop updates target lane",
        "attachment drop zones accept small files",
        "CARD_SELECTOR",
        "SOURCE_LANE_SELECTOR",
        "TARGET_LANE_SELECTOR",
        "ATTACHMENT_ZONE_SELECTORS",
        "#createAttachmentDropZone",
        "#issueAttachmentDropZone",
        "#commentAttachmentDropZone",
        "prevented.defaultPrevented",
    ] {
        assert!(
            spec.contains(required),
            "Playwright spec missing {required}"
        );
    }

    for required in [
        "startFixtureServer",
        "127.0.0.1",
        "LUC-654-FIXTURE",
        r#"data-lane="${lane}""#,
        "Queue",
        "Active",
        "window.__luc654.moves.push",
        "window.__luc654.uploads.push",
        "document.addEventListener('drop'",
        "At most 20 attachments can be added.",
        "25 MiB per-file attachment limit",
        "100 MiB total attachment limit",
    ] {
        assert!(
            fixture.contains(required),
            "fixture server missing {required}"
        );
    }

    for required in [
        "test:luc654-ui",
        "playwright test tests/luc654-ui-validation.spec.mjs --project=chromium",
        "@playwright/test",
    ] {
        assert!(
            package.contains(required),
            "package.json missing {required}"
        );
    }

    for required in [
        "defineConfig",
        "name: 'chromium'",
        "reporter: [['list']]",
        "trace: 'retain-on-failure'",
    ] {
        assert!(
            config.contains(required),
            "playwright config missing {required}"
        );
    }
}
