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

/// Draw an integer in `0..n`, identically on every target.
///
/// `fastrand`'s `usize` methods dispatch on `target_pointer_width` — 32-bit
/// builds go through `gen_mod_u32`, 64-bit through `gen_mod_u64` — so the same
/// seed yields different values on different architectures, and every
/// subsequent draw diverges from there. `u32` has one implementation
/// everywhere. Every draw in this module goes through this function or
/// `rng.i64`/`rng.u32` directly, so the crate's promise of identical bytes on
/// every machine holds off 64-bit as well as on it.
fn below(rng: &mut fastrand::Rng, n: usize) -> usize {
    rng.u32(..u32::try_from(n).expect("scenario draw bounds fit in u32")) as usize
}

/// Pick an element of `items` with an architecture-independent draw.
fn pick<'a, T>(rng: &mut fastrand::Rng, items: &'a [T]) -> &'a T {
    &items[below(rng, items.len())]
}

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
        let first = *pick(&mut rng, FIRST_NAMES);
        let last = *pick(&mut rng, LAST_NAMES);
        let domain = *pick(&mut rng, EMAIL_DOMAINS);
        let (sku, _, _, price_cents) = *pick(&mut rng, CATALOGUE);
        let channel = *pick(&mut rng, CHANNELS);
        let country = *pick(&mut rng, SHIP_COUNTRIES);

        // Weighted so most orders are healthy: the last two statuses
        // (cancelled, refunded) appear roughly one row in six.
        let status = if below(&mut rng, 6) == 0 {
            ORDER_STATUSES[3 + below(&mut rng, 2)]
        } else {
            ORDER_STATUSES[below(&mut rng, 3)]
        };

        let qty = 1 + below(&mut rng, 4);
        let discount = [0, 0, 0, 5, 10, 15, 25][below(&mut rng, 7)];

        csv_row(
            &mut out,
            &[
                &format!("SO-{:05}", 10_000 + i),
                &day(rng.i64(0..60)),
                &format!("C-{:04}", 1000 + below(&mut rng, 240)),
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

/// Scenario 02 — a supplier product feed as XML with repeated elements.
///
/// Each `<product>` carries **one or more** `<category>` elements. Those repeats
/// are the point: they exercise a `multiple: true` column end to end, read as
/// one multi-value field and written back out as repeated elements.
///
/// The repeated element is a *direct* child of the record element rather than
/// sitting inside a `<categories>` container, and money is a flat
/// `<list_price_minor>` rather than a nested `<pricing>` block. Both are
/// deliberate: a nested element flattens to a dotted column name that CXL
/// cannot currently address (#995), so a wrapped repeat would be readable but
/// untransformable. The write side reintroduces the container via `wrap_in`.
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
            let cat_count = 1 + below(&mut rng, 3);
            let mut chosen: Vec<&str> = Vec::with_capacity(cat_count);
            while chosen.len() < cat_count {
                let c = *pick(&mut rng, CATEGORIES);
                if !chosen.contains(&c) {
                    chosen.push(c);
                }
            }

            out.push_str(&format!("  <product sku=\"{}\">\n", xml_escape(&vsku)));
            xml_leaf(&mut out, 2, "name", &vname);
            xml_leaf(&mut out, 2, "brand", brand);
            // Money travels as integer minor units, which is both good practice
            // and currently necessary: the XML reader type-infers element text
            // and ignores the declared column type, so `24.50` in a `decimal`
            // column arrives as a float expansion (#992). An integer element
            // reads back exactly, and the scenario derives the display amount
            // from it in decimal.
            xml_leaf(&mut out, 2, "list_price_minor", &price_cents.to_string());
            xml_leaf(
                &mut out,
                2,
                "cost_minor",
                &(price_cents * 6 / 10).to_string(),
            );
            // Repeated directly under <product>: a container would flatten to a
            // dotted column name CXL cannot address (#995).
            for c in &chosen {
                xml_leaf(&mut out, 2, "category", c);
            }
            xml_leaf(
                &mut out,
                2,
                "stock_on_hand",
                &below(&mut rng, 500).to_string(),
            );
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
        let first = *pick(&mut rng, FIRST_NAMES);
        let last = *pick(&mut rng, LAST_NAMES);
        let domain = *pick(&mut rng, EMAIL_DOMAINS);
        let category = *pick(&mut rng, TICKET_CATEGORIES);
        let priority = *pick(&mut rng, RAW_PRIORITIES);
        let locality = *pick(&mut rng, LOCALITIES);

        // Roughly one row in ten carries an unparseable response time.
        let response = if below(&mut rng, 10) == 0 {
            pick(&mut rng, BAD_RESPONSE).to_string()
        } else {
            (2 + below(&mut rng, 478)).to_string()
        };

        let satisfaction = if below(&mut rng, 8) == 0 {
            String::new()
        } else {
            (1 + below(&mut rng, 5)).to_string()
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

/// Scenario 04 — per-file source verification and exact terminal ordering.
///
/// The first CSV already follows the declared `(account_id, batch_seq)` order.
/// The second contains one adjacent inversion, so the default warn policy must
/// repair that physical file without treating the two-file source as one global
/// sequence. The output then authors a different total business order.
pub fn ordering_contract() -> GeneratedData {
    let sorted = concat!(
        "account_id,batch_seq,region,priority,event_id,amount_cents\n",
        "ACCT-100,1,north,2,EVT-1001,1250\n",
        "ACCT-100,2,south,5,EVT-1002,2300\n",
        "ACCT-200,1,east,3,EVT-2001,4999\n",
        "ACCT-200,2,north,5,EVT-2002,1750\n",
        "ACCT-300,1,south,1,EVT-3001,820\n",
        "ACCT-300,2,east,4,EVT-3002,6400\n",
        "ACCT-400,1,north,3,EVT-4001,3100\n",
        "ACCT-400,2,east,5,EVT-4002,2700\n",
        "ACCT-500,1,south,4,EVT-5001,1500\n",
        "ACCT-500,2,north,1,EVT-5002,910\n",
        "ACCT-600,1,east,2,EVT-6001,3600\n",
        "ACCT-600,2,south,3,EVT-6002,4200\n",
    );
    let needs_repair = concat!(
        "account_id,batch_seq,region,priority,event_id,amount_cents\n",
        "ACCT-100,3,east,5,EVT-1003,5500\n",
        "ACCT-100,4,north,4,EVT-1004,1325\n",
        "ACCT-200,3,south,2,EVT-2003,2875\n",
        "ACCT-200,4,east,1,EVT-2004,940\n",
        "ACCT-300,4,north,2,EVT-3004,7600\n",
        "ACCT-300,3,south,5,EVT-3003,5100\n",
        "ACCT-400,3,east,4,EVT-4003,2250\n",
        "ACCT-400,4,south,1,EVT-4004,1180\n",
        "ACCT-500,3,north,5,EVT-5003,6800\n",
        "ACCT-500,4,east,3,EVT-5004,3425\n",
        "ACCT-600,3,south,4,EVT-6003,1995\n",
        "ACCT-600,4,north,3,EVT-6004,4550\n",
    );

    GeneratedData::new(vec![
        GeneratedFile {
            path: "01-sorted.csv",
            bytes: sorted.as_bytes().to_vec(),
        },
        GeneratedFile {
            path: "02-needs-repair.csv",
            bytes: needs_repair.as_bytes().to_vec(),
        },
    ])
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

    /// The `<category>` values of each `<product>` block, in document order.
    ///
    /// Splits on the record element rather than a container: the repeats are
    /// direct children of `<product>`, so there is no wrapper to key on. A
    /// helper shared by the tests below keeps them from silently going vacuous
    /// if the feed's shape changes again — an empty result fails
    /// `product_feed_emits_categories_at_all`.
    fn categories_per_product(xml: &str) -> Vec<Vec<&str>> {
        xml.split("<product ")
            .skip(1)
            .map(|block| {
                let block = block.split("</product>").next().unwrap_or(block);
                block
                    .lines()
                    .filter_map(|l| l.trim().strip_prefix("<category>"))
                    .filter_map(|l| l.strip_suffix("</category>"))
                    .collect()
            })
            .collect()
    }

    #[test]
    fn product_feed_emits_categories_at_all() {
        // Guards the two tests below from passing vacuously: if the feed's
        // shape changes so the parser matches nothing, they would otherwise
        // both succeed on an empty set.
        let xml = String::from_utf8(product_feed().files()[0].bytes.clone()).unwrap();
        let per_product = categories_per_product(&xml);
        assert!(!per_product.is_empty(), "no <product> blocks parsed");
        assert!(
            per_product.iter().all(|c| !c.is_empty()),
            "every product must carry at least one category"
        );
    }

    #[test]
    fn product_feed_has_products_with_more_than_one_category() {
        // The repeated <category> elements are the whole reason this scenario
        // exists; a generator that emitted exactly one per product would leave
        // the multi-value path unexercised.
        let xml = String::from_utf8(product_feed().files()[0].bytes.clone()).unwrap();
        let multi = categories_per_product(&xml)
            .iter()
            .filter(|c| c.len() > 1)
            .count();
        assert!(multi > 0, "no product carries repeated <category> elements");
    }

    #[test]
    fn product_feed_never_repeats_a_category_within_one_product() {
        let xml = String::from_utf8(product_feed().files()[0].bytes.clone()).unwrap();
        for mut cats in categories_per_product(&xml) {
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

    fn ordering_key(row: &str) -> (&str, u64) {
        let mut columns = row.split(',');
        let account_id = columns.next().expect("account_id");
        let batch_seq = columns
            .next()
            .expect("batch_seq")
            .parse()
            .expect("numeric batch_seq");
        (account_id, batch_seq)
    }

    fn is_source_ordered(lines: &[String]) -> bool {
        lines
            .windows(2)
            .all(|pair| ordering_key(&pair[0]) <= ordering_key(&pair[1]))
    }

    #[test]
    fn ordering_contract_input_is_deterministic_with_one_repair_case() {
        let first = ordering_contract();
        let second = ordering_contract();

        assert_eq!(first, second, "ordering input must be byte-deterministic");
        assert_eq!(
            first.digest(),
            "8b891035785d20d9b97616706273f1ab4ccc1b55abf4c2b03083abd3a2d598a2",
            "the committed golden is meaningful only for this input digest"
        );
        assert_eq!(
            first
                .files()
                .iter()
                .map(|file| file.path)
                .collect::<Vec<_>>(),
            ["01-sorted.csv", "02-needs-repair.csv"]
        );

        let sorted = csv_lines(&first, "01-sorted.csv");
        let needs_repair = csv_lines(&first, "02-needs-repair.csv");
        assert!(is_source_ordered(&sorted[1..]), "first file must be sorted");
        assert!(
            !is_source_ordered(&needs_repair[1..]),
            "second file must contain an inversion"
        );
        assert_eq!(
            needs_repair[1..]
                .windows(2)
                .filter(|pair| ordering_key(&pair[0]) > ordering_key(&pair[1]))
                .count(),
            1,
            "the repair case should contain exactly one adjacent inversion"
        );
    }

    #[test]
    fn ordering_contract_pipeline_declares_source_and_terminal_order() {
        let pipeline =
            include_str!("../../../examples/scenarios/04-ordering-contract/pipeline.yaml");

        assert!(
            pipeline.contains(
                "paths:\n        - ./data/01-sorted.csv\n        - ./data/02-needs-repair.csv"
            ),
            "the source must consume the two physical files explicitly"
        );
        assert!(
            pipeline.contains("sort_order:\n        - account_id\n        - batch_seq"),
            "the source must declare its per-file order"
        );
        assert!(
            !pipeline.contains("on_unsorted:"),
            "omission must exercise the default warn-and-repair policy"
        );
        assert!(
            pipeline.contains(
                "sort_order:\n        - { field: region, order: asc }\n        - { field: priority, order: desc }\n        - { field: account_id, order: asc }\n        - { field: batch_seq, order: asc }\n        - { field: event_id, order: asc }"
            ),
            "the terminal output must author a total business order"
        );
    }

    #[test]
    fn ordering_contract_golden_is_the_repaired_total_order() {
        let data = ordering_contract();
        let mut rows = data
            .files()
            .iter()
            .flat_map(|file| {
                String::from_utf8(file.bytes.clone())
                    .expect("ordering input is UTF-8")
                    .lines()
                    .skip(1)
                    .map(str::to_owned)
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();

        rows.sort_by(|left, right| {
            let left = left.split(',').collect::<Vec<_>>();
            let right = right.split(',').collect::<Vec<_>>();
            left[2]
                .cmp(right[2])
                .then_with(|| {
                    right[3]
                        .parse::<u64>()
                        .expect("numeric priority")
                        .cmp(&left[3].parse::<u64>().expect("numeric priority"))
                })
                .then_with(|| left[0].cmp(right[0]))
                .then_with(|| {
                    left[1]
                        .parse::<u64>()
                        .expect("numeric batch_seq")
                        .cmp(&right[1].parse::<u64>().expect("numeric batch_seq"))
                })
                .then_with(|| left[4].cmp(right[4]))
        });

        let mut derived =
            String::from("account_id,batch_seq,region,priority,event_id,amount_cents\n");
        derived.push_str(&rows.join("\n"));
        derived.push('\n');
        assert_eq!(
            include_str!("../../../examples/scenarios/04-ordering-contract/expected/ordered.csv"),
            derived,
            "the golden must be the repaired input under the authored total order"
        );
    }
}
