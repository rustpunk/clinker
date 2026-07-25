//! Fictional value vocabulary for generated scenario data.
//!
//! # Why this module exists
//!
//! The scenarios deliberately keep PII-*shaped* fields — `customer_name`,
//! `email`, `date_of_birth`. Stripping them would gut the demos, because the
//! reader's whole job is mapping their own customer file onto one of these
//! pipelines, and a corpus of `f0`/`f1`/`f2` columns teaches nothing about that.
//!
//! The risk is never the schema. It is a *value* that collides with a real
//! person or validates against a real system. Because every byte of scenario
//! data originates here, that risk is confined to one reviewable file rather
//! than scattered across dozens of committed CSVs.
//!
//! # Policy — every value below comes from a reserved fictional range
//!
//! - **Email domains**: RFC 2606 reserved (`example.com`, `example.org`,
//!   `.invalid`, `.test`) only. Never `acme.com`, `contoso.com`, or any other
//!   domain a third party actually owns.
//! - **Phone numbers**: the NANP fiction range 555-0100 through 555-0199.
//! - **IP addresses**: RFC 5737 documentation blocks.
//! - **Government identifiers**: none. An opaque `employee_id` carries identity
//!   instead, which is also the better modelling choice.
//! - **Payment cards**: published test PANs only, never an arbitrary
//!   Luhn-valid number, which can collide with a live card.
//! - **Company, person, and street names**: invented. Not borrowed from the
//!   usual sample-data casts, which belong to specific vendors.
//!
//! # Determinism
//!
//! These are `const` tables, not a third-party faker. A faker's data tables can
//! shift between minor releases, which would silently re-bless every committed
//! golden; owning the vocabulary keeps golden stability under this repo's
//! control. Changing any table below is a [`crate::GENERATOR_VERSION`] event.

/// Given names. Paired positionally with nothing — callers index independently
/// so the surname distribution is not correlated.
pub const FIRST_NAMES: &[&str] = &[
    "Dana", "Reza", "Ingrid", "Mateo", "Priya", "Callum", "Nadia", "Tomas", "Aoife", "Kwame",
    "Lena", "Rafael", "Sunniva", "Idris", "Marta", "Ewan", "Chiara", "Bo", "Freya", "Nikolai",
    "Amara", "Joaquin", "Saskia", "Emeka",
];

/// Surnames. Invented or sufficiently generic that no specific individual is
/// implied when paired with a given name above.
pub const LAST_NAMES: &[&str] = &[
    "Whitfield",
    "Ashgrove",
    "Petrov",
    "Nakamura",
    "Oyelaran",
    "Lindqvist",
    "Marchetti",
    "Dunmore",
    "Kaur",
    "Bergstrom",
    "Okonkwo",
    "Ferreira",
    "Halloran",
    "Voss",
    "Nikolaidis",
    "Cardenas",
    "Thackeray",
    "Ilves",
];

/// RFC 2606 reserved domains. Every generated address must use one of these.
pub const EMAIL_DOMAINS: &[&str] = &["example.com", "example.org", "example.net"];

/// Invented trading names. Deliberately not drawn from the standard vendor
/// sample casts (Northwind, Contoso, AdventureWorks), which are third-party
/// marks even where their use is tolerated.
pub const COMPANY_NAMES: &[&str] = &[
    "Harbourline Supply",
    "Verity Foods",
    "Kestrel Instruments",
    "Northgate Textiles",
    "Aldercroft Logistics",
    "Bramblewick Trading",
    "Fenmoor Components",
    "Saltmarsh Provisions",
];

/// Invented street names, combined with a generated house number.
pub const STREET_NAMES: &[&str] = &[
    "Alder Row",
    "Kiln Lane",
    "Quarry Rise",
    "Netherfield Way",
    "Cobb Street",
    "Marlpit Close",
    "Tern Walk",
    "Foundry Gate",
    "Bracken Hill",
    "Wharf End",
];

/// City / region / postal-code triples. Postal codes are structurally valid for
/// their format but chosen from unassigned or documentation-safe ranges.
pub const LOCALITIES: &[(&str, &str, &str)] = &[
    ("Ashford Bay", "OR", "97099"),
    ("Kestrel Falls", "CO", "80999"),
    ("Northgate", "NY", "10999"),
    ("Saltmarsh", "ME", "04999"),
    ("Bramblewick", "VT", "05999"),
    ("Fenmoor", "WA", "98999"),
];

/// Sales channels for the storefront scenario.
pub const CHANNELS: &[&str] = &["web", "mobile", "phone", "partner"];

/// Order lifecycle states. `cancelled` and `refunded` exist so a scenario has
/// something meaningful to filter on rather than a synthetic flag.
pub const ORDER_STATUSES: &[&str] = &["placed", "shipped", "delivered", "cancelled", "refunded"];

/// ISO 3166-1 alpha-2 codes for destinations used across scenarios.
pub const SHIP_COUNTRIES: &[&str] = &["US", "CA", "GB", "DE", "FR", "JP", "AU"];

/// Catalogue entries: `(sku, product_name, brand, unit_price_cents)`.
///
/// Prices are integer cents throughout. Money never passes through an `f64` in
/// generated data — a float would make byte-exact goldens depend on formatting
/// behaviour rather than on pipeline logic.
pub const CATALOGUE: &[(&str, &str, &str, i64)] = &[
    ("HL-1001", "Insulated Flask 750ml", "Kestrel", 2450),
    ("HL-1002", "Trail Mug Enamel", "Kestrel", 1195),
    ("NT-2010", "Merino Base Layer", "Northgate", 6800),
    ("NT-2011", "Wool Blend Socks 3pk", "Northgate", 2200),
    ("VF-3050", "Cold Brew Concentrate 1L", "Verity", 1650),
    ("VF-3051", "Single Origin Beans 500g", "Verity", 1899),
    ("FC-4100", "Bearing Assembly M8", "Fenmoor", 940),
    ("FC-4101", "Drive Belt 1200mm", "Fenmoor", 3120),
    ("SP-5000", "Sea Salt Flakes 250g", "Saltmarsh", 720),
    ("SP-5001", "Smoked Paprika 90g", "Saltmarsh", 545),
];

/// Product categories. A product carries one *or more* of these, which is what
/// makes the product feed a natural demonstration of a `multiple: true` column
/// emitting repeated XML child elements.
pub const CATEGORIES: &[&str] = &[
    "outdoor",
    "drinkware",
    "apparel",
    "grocery",
    "industrial",
    "gift",
    "clearance",
    "seasonal",
];

/// Support ticket categories.
pub const TICKET_CATEGORIES: &[&str] = &[
    "billing",
    "shipping",
    "product-defect",
    "account-access",
    "returns",
    "other",
];

/// Support ticket priorities as they arrive from the upstream helpdesk export —
/// deliberately mixed-case and inconsistent, because normalising them is part of
/// what the triage scenario demonstrates.
pub const RAW_PRIORITIES: &[&str] = &["P1", "p1", "High", "P2", "Normal", "p3", "Low", "URGENT"];

/// Build an email local part from a name pair, lowercased and dot-separated.
///
/// Returns only the local part; callers append a domain from [`EMAIL_DOMAINS`]
/// so no call site can accidentally invent an unreserved domain.
pub fn email_local(first: &str, last: &str) -> String {
    let mut s = String::with_capacity(first.len() + last.len() + 1);
    for ch in first.chars() {
        s.extend(ch.to_lowercase());
    }
    s.push('.');
    for ch in last.chars() {
        s.extend(ch.to_lowercase());
    }
    s
}

/// A phone number inside the NANP fiction range (555-0100..555-0199).
///
/// `seq` is taken modulo 100, so every value this can produce stays inside the
/// reserved block regardless of what the caller passes.
pub fn fiction_phone(area: u16, seq: u16) -> String {
    format!("({area}) 555-{:04}", 100 + (seq % 100))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_email_domain_is_rfc2606_reserved() {
        // The policy this module exists to enforce. A domain added here that is
        // registrable by a third party would put a live address in public
        // example data.
        for d in EMAIL_DOMAINS {
            assert!(
                matches!(*d, "example.com" | "example.org" | "example.net")
                    || d.ends_with(".invalid")
                    || d.ends_with(".test"),
                "email domain {d} is not an RFC 2606 reserved name"
            );
        }
    }

    #[test]
    fn fiction_phone_never_leaves_the_reserved_block() {
        // Guards the modulo: any seq, including ones far past the block size,
        // must still land in 555-0100..555-0199.
        for seq in [0u16, 1, 99, 100, 101, 65535] {
            let p = fiction_phone(503, seq);
            let last4: u32 = p[p.len() - 4..].parse().expect("trailing 4 digits");
            assert!(
                (100..=199).contains(&last4),
                "phone {p} escaped the 555-0100..555-0199 fiction range"
            );
        }
    }

    #[test]
    fn catalogue_prices_are_integer_cents() {
        // A zero or negative price would make discount arithmetic in the
        // scenarios meaningless, and floats are barred outright.
        for (sku, _, _, cents) in CATALOGUE {
            assert!(*cents > 0, "{sku} has a non-positive price");
        }
    }

    #[test]
    fn email_local_is_lowercase_and_dotted() {
        assert_eq!(email_local("Dana", "Whitfield"), "dana.whitfield");
    }
}
