//! Per-scenario input generators.
//!
//! Every function here is pure: seeded from a per-scenario constant, drawing
//! only from [`crate::vocab`], emitting through [`crate::emit`]. Calling one
//! twice on any machine yields identical bytes — the property
//! `crate::tests::every_scenario_generates_identical_bytes_on_repeat_calls`
//! defends, and the reason a committed golden means anything.
//!
//! Row counts sit in the 25-80 range so a reader can hold the whole input and
//! the whole expected output in view at once. That readability is the point:
//! these files are documentation that happens to also be a test fixture.

use crate::emit::{csv_row, day, money, timestamp, xml_escape, xml_leaf};
use crate::vocab::{
    CATALOGUE, CATEGORIES, CHANNELS, COMPANY_NAMES, EMAIL_DOMAINS, FIRST_NAMES, LAST_NAMES,
    LOCALITIES, ORDER_STATUSES, RAW_PRIORITIES, SHIP_COUNTRIES, TICKET_CATEGORIES, email_local,
};
use crate::{GeneratedData, GeneratedFile};

/// Per-scenario seeds. Distinct values keep two scenarios from accidentally
/// drawing the same sequence and producing look-alike data.
const SEED_STOREFRONT: u64 = 0x5701_0001;
const SEED_PRODUCT_FEED: u64 = 0x5701_0002;
const SEED_SUPPORT: u64 = 0x5701_0003;

/// Scenario 01 — a storefront order export.
///
/// One CSV of order lines. Carries cancelled and refunded rows so the pipeline's
/// filter has real work, a spread of discounts so the derived line total is not
/// trivially the unit price, and prices as integer-cent strings.
pub fn storefront_orders() -> GeneratedData {
    const ROWS: usize = 48;
    let mut rng = fastrand::Rng::with_seed(SEED_STOREFRONT);
    let mut out = String::new();

    csv_row(
        &mut out,
        &[
            "order_id",
            "order_date",
            "customer_id",
            "customer_name",
            "customer_email",
            "channel",
            "sku",
            "quantity",
            "unit_price",
            "discount_pct",
            "ship_country",
            "status",
        ],
    );

    for i in 0..ROWS {
        let first = FIRST_NAMES[rng.usize(..FIRST_NAMES.len())];
        let last = LAST_NAMES[rng.usize(..LAST_NAMES.len())];
        let domain = EMAIL_DOMAINS[rng.usize(..EMAIL_DOMAINS.len())];
        let (sku, _, _, price_cents) = CATALOGUE[rng.usize(..CATALOGUE.len())];
        let channel = CHANNELS[rng.usize(..CHANNELS.len())];
        let country = SHIP_COUNTRIES[rng.usize(..SHIP_COUNTRIES.len())];

        // Weighted so most orders are healthy: the last two statuses
        // (cancelled, refunded) appear roughly one row in six.
        let status = if rng.usize(..6) == 0 {
            ORDER_STATUSES[3 + rng.usize(..2)]
        } else {
            ORDER_STATUSES[rng.usize(..3)]
        };

        let qty = 1 + rng.usize(..4);
        let discount = [0, 0, 0, 5, 10, 15, 25][rng.usize(..7)];

        csv_row(
            &mut out,
            &[
                &format!("SO-{:05}", 10_000 + i),
                &day(rng.i64(0..60)),
                &format!("C-{:04}", 1000 + rng.usize(..240)),
                &format!("{first} {last}"),
                &format!("{}@{domain}", email_local(first, last)),
                channel,
                sku,
                &qty.to_string(),
                &money(price_cents),
                &discount.to_string(),
                country,
                status,
            ],
        );
    }

    GeneratedData::new(vec![GeneratedFile {
        path: "orders.csv",
        bytes: out.into_bytes(),
    }])
}

/// Scenario 02 — a supplier product feed as nested XML.
///
/// Each `<product>` carries a nested `<pricing>` block and a `<categories>`
/// block holding **one or more** `<category>` elements. Those repeats are the
/// point: they exercise a `multiple: true` column end to end, read as one
/// multi-value field and written back out as repeated elements.
pub fn product_feed() -> GeneratedData {
    let mut rng = fastrand::Rng::with_seed(SEED_PRODUCT_FEED);
    let mut out = String::new();

    out.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    out.push_str(&format!(
        "<catalog supplier=\"{}\" feed_date=\"{}\" currency=\"USD\">\n",
        xml_escape(COMPANY_NAMES[0]),
        day(0)
    ));

    // Every catalogue entry appears, plus a second variant for some, so the feed
    // is larger than the catalogue without inventing unrelated products.
    for (idx, (sku, name, brand, price_cents)) in CATALOGUE.iter().enumerate() {
        let variants = if idx % 3 == 0 { 2 } else { 1 };
        for v in 0..variants {
            let vsku = if v == 0 {
                (*sku).to_string()
            } else {
                format!("{sku}-R2")
            };
            let vname = if v == 0 {
                (*name).to_string()
            } else {
                format!("{name} (Refill)")
            };
            // Deterministic 1..=3 categories, never repeating within a product.
            let cat_count = 1 + rng.usize(..3);
            let mut chosen: Vec<&str> = Vec::with_capacity(cat_count);
            while chosen.len() < cat_count {
                let c = CATEGORIES[rng.usize(..CATEGORIES.len())];
                if !chosen.contains(&c) {
                    chosen.push(c);
                }
            }

            out.push_str(&format!("  <product sku=\"{}\">\n", xml_escape(&vsku)));
            xml_leaf(&mut out, 2, "name", &vname);
            xml_leaf(&mut out, 2, "brand", brand);
            out.push_str("    <pricing>\n");
            xml_leaf(&mut out, 3, "list_price", &money(*price_cents));
            xml_leaf(&mut out, 3, "cost", &money(price_cents * 6 / 10));
            out.push_str("    </pricing>\n");
            out.push_str("    <categories>\n");
            for c in &chosen {
                xml_leaf(&mut out, 3, "category", c);
            }
            out.push_str("    </categories>\n");
            xml_leaf(&mut out, 2, "stock_on_hand", &rng.usize(0..500).to_string());
            out.push_str("  </product>\n");
        }
    }

    out.push_str("</catalog>\n");

    GeneratedData::new(vec![GeneratedFile {
        path: "catalog.xml",
        bytes: out.into_bytes(),
    }])
}

/// Scenario 03 — a helpdesk ticket export needing triage.
///
/// Priorities arrive inconsistently cased and abbreviated, which the pipeline
/// normalises. A deliberate minority of rows carry a `first_response_mins` value
/// that is not an integer — the real shape of an export where an agent typed
/// free text into a numeric field. Those rows are the dead-letter population,
/// and they are what makes the DLQ demonstration honest rather than staged.
pub fn support_triage() -> GeneratedData {
    const ROWS: usize = 60;
    /// Non-numeric values a real helpdesk export puts in a numeric column.
    const BAD_RESPONSE: &[&str] = &["n/a", "pending", "--", ""];

    let mut rng = fastrand::Rng::with_seed(SEED_SUPPORT);
    let mut out = String::new();

    csv_row(
        &mut out,
        &[
            "ticket_id",
            "opened_at",
            "customer_email",
            "raw_priority",
            "category",
            "subject",
            "first_response_mins",
            "satisfaction",
        ],
    );

    for i in 0..ROWS {
        let first = FIRST_NAMES[rng.usize(..FIRST_NAMES.len())];
        let last = LAST_NAMES[rng.usize(..LAST_NAMES.len())];
        let domain = EMAIL_DOMAINS[rng.usize(..EMAIL_DOMAINS.len())];
        let category = TICKET_CATEGORIES[rng.usize(..TICKET_CATEGORIES.len())];
        let priority = RAW_PRIORITIES[rng.usize(..RAW_PRIORITIES.len())];
        let locality = LOCALITIES[rng.usize(..LOCALITIES.len())];

        // Roughly one row in ten carries an unparseable response time.
        let response = if rng.usize(..10) == 0 {
            BAD_RESPONSE[rng.usize(..BAD_RESPONSE.len())].to_string()
        } else {
            rng.usize(2..480).to_string()
        };

        let satisfaction = if rng.usize(..8) == 0 {
            String::new()
        } else {
            (1 + rng.usize(..5)).to_string()
        };

        csv_row(
            &mut out,
            &[
                &format!("TK-{:06}", 200_000 + i),
                &timestamp(rng.i64(0..30), rng.u32(7..19), rng.u32(0..60), 0),
                &format!("{}@{domain}", email_local(first, last)),
                priority,
                category,
                &format!("{} issue reported from {}", category, locality.0),
                &response,
                &satisfaction,
            ],
        );
    }

    GeneratedData::new(vec![GeneratedFile {
        path: "tickets.csv",
        bytes: out.into_bytes(),
    }])
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Split a generated CSV into header and data lines.
    fn csv_lines(data: &GeneratedData, path: &str) -> Vec<String> {
        let f = data
            .files()
            .iter()
            .find(|f| f.path == path)
            .unwrap_or_else(|| panic!("{path} missing"));
        String::from_utf8(f.bytes.clone())
            .unwrap()
            .lines()
            .map(str::to_owned)
            .collect()
    }

    #[test]
    fn storefront_has_a_header_and_the_declared_row_count() {
        let lines = csv_lines(&storefront_orders(), "orders.csv");
        assert_eq!(lines.len(), 49, "48 data rows plus one header");
        assert!(lines[0].starts_with("order_id,order_date,"));
    }

    #[test]
    fn storefront_contains_both_filterable_and_retained_statuses() {
        // The scenario's filter is only meaningful if the input has rows on both
        // sides of it. A generator drift that produced no cancellations would
        // make the pipeline look correct while testing nothing.
        let body = String::from_utf8(storefront_orders().files()[0].bytes.clone()).unwrap();
        for status in ["placed", "cancelled"] {
            assert!(
                body.lines().skip(1).any(|l| l.ends_with(status)),
                "no row with status {status}"
            );
        }
    }

    #[test]
    fn storefront_emails_use_only_reserved_domains() {
        let body = String::from_utf8(storefront_orders().files()[0].bytes.clone()).unwrap();
        for line in body.lines().skip(1) {
            let email = line.split(',').nth(4).expect("email column");
            let domain = email.rsplit('@').next().expect("domain");
            assert!(
                EMAIL_DOMAINS.contains(&domain),
                "unreserved email domain in generated data: {domain}"
            );
        }
    }

    #[test]
    fn product_feed_has_products_with_more_than_one_category() {
        // The repeated <category> elements are the whole reason this scenario
        // exists; a generator that emitted exactly one per product would leave
        // the multi-value path unexercised.
        let xml = String::from_utf8(product_feed().files()[0].bytes.clone()).unwrap();
        let multi = xml
            .split("<categories>")
            .skip(1)
            .filter(|block| block.matches("<category>").count() > 1)
            .count();
        assert!(multi > 0, "no product carries repeated <category> elements");
    }

    #[test]
    fn product_feed_never_repeats_a_category_within_one_product() {
        let xml = String::from_utf8(product_feed().files()[0].bytes.clone()).unwrap();
        for block in xml.split("<categories>").skip(1) {
            let block = block.split("</categories>").next().unwrap();
            let mut cats: Vec<&str> = block
                .lines()
                .filter_map(|l| l.trim().strip_prefix("<category>"))
                .filter_map(|l| l.strip_suffix("</category>"))
                .collect();
            let before = cats.len();
            cats.sort_unstable();
            cats.dedup();
            assert_eq!(before, cats.len(), "duplicate category within one product");
        }
    }

    #[test]
    fn support_export_carries_unparseable_response_times() {
        // These rows are the dead-letter population. Without them the DLQ sink
        // would be empty and the scenario would assert nothing about it.
        let lines = csv_lines(&support_triage(), "tickets.csv");
        let bad = lines
            .iter()
            .skip(1)
            .filter(|l| {
                let v = l.split(',').nth(6).unwrap_or("0");
                v.parse::<i64>().is_err()
            })
            .count();
        assert!(bad > 0, "no unparseable first_response_mins rows generated");
        assert!(
            bad < lines.len() / 3,
            "unparseable rows should be a minority, got {bad} of {}",
            lines.len() - 1
        );
    }

    #[test]
    fn support_export_has_mixed_case_priorities_to_normalise() {
        let body = String::from_utf8(support_triage().files()[0].bytes.clone()).unwrap();
        let has_upper = body.lines().skip(1).any(|l| l.contains(",P1,"));
        let has_lower = body.lines().skip(1).any(|l| l.contains(",p1,"));
        assert!(
            has_upper && has_lower,
            "priority normalisation needs both cases present"
        );
    }
}
