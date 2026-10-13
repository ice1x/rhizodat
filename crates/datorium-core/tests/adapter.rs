//! The adapter registry: the core knows no store; adapters declare their DSN
//! schemes, and a registry built from them parses connection strings.

use datorium_core::adapter::{Adapter, FILE, Presence, Registry, RegistryError, Scheme};

const KV: Adapter = Adapter {
    kind: "kv",
    schemes: &[
        Scheme {
            name: "kv",
            transport: None,
            default_port: Some(7000),
            host: Presence::Required,
            path: Presence::Optional,
        },
        Scheme {
            name: "kv+file",
            transport: Some("file"),
            default_port: None,
            host: Presence::None,
            path: Presence::Required,
        },
        Scheme {
            name: "kv+mem",
            transport: Some("memory"),
            default_port: None,
            host: Presence::None,
            path: Presence::None,
        },
    ],
    holds_journal: false,
    query_help: Some("GET <key>"),
};

fn kv_with(kind: &'static str, schemes: &'static [Scheme]) -> Adapter {
    Adapter {
        kind,
        schemes,
        ..KV
    }
}

#[test]
fn a_new_registry_knows_only_the_file_journal() {
    let registry = Registry::new();
    let kinds: Vec<&str> = registry.adapters().iter().map(|a| a.kind).collect();
    assert_eq!(kinds, ["file"]);
    assert_eq!(registry.parse("file:///tmp/j.db").unwrap().store(), "file");
    let err = registry.parse("postgresql://db/app").unwrap_err();
    assert!(err.to_string().contains("supported: file://"), "{err}");
}

#[test]
fn a_registered_adapter_parses_its_schemes_with_their_rules() {
    let registry = Registry::new().with(KV).unwrap();
    let d = registry.parse("kv://cache.local/space").unwrap();
    assert_eq!(
        (d.store(), d.host(), d.port(), d.database()),
        ("kv", Some("cache.local"), Some(7000), Some("space"))
    );
    assert_eq!(
        registry.parse("kv+file:///data/x.kv").unwrap().transport(),
        Some("file")
    );
    assert_eq!(
        registry.parse("kv+mem://").unwrap().transport(),
        Some("memory")
    );
    for bad in [
        "kv:///space",
        "kv+file://h/x",
        "kv+file://",
        "kv+mem://h",
        "kv+mem:///x",
    ] {
        assert!(registry.parse(bad).is_err(), "{bad}");
    }
    assert_eq!(
        registry.adapter("kv").unwrap().query_help,
        Some("GET <key>")
    );
    assert!(registry.adapter("mysql").is_none());
}

#[test]
fn an_adapter_parses_only_its_own_schemes() {
    assert_eq!(KV.parse("kv://h/x").unwrap().store(), "kv");
    let err = KV.parse("file:///tmp/x").unwrap_err();
    assert!(err.to_string().contains("supported: kv://"), "{err}");
    assert!(FILE.parse("file:///tmp/x").is_ok());
}

#[test]
fn ambiguous_or_invalid_adapters_are_refused() {
    const TAKEN: &[Scheme] = &[Scheme {
        name: "file",
        transport: None,
        default_port: None,
        host: Presence::Optional,
        path: Presence::Optional,
    }];
    const TWICE: &[Scheme] = &[KV.schemes[0], KV.schemes[0]];
    const BAD: &[Scheme] = &[Scheme {
        name: "kv:x",
        ..KV.schemes[0]
    }];
    let cases = [
        (kv_with("file", KV.schemes), "already registered"),
        (
            kv_with("kv", TAKEN),
            "already belongs to the \"file\" adapter",
        ),
        (kv_with("kv", TWICE), "twice"),
        (kv_with("kv", &[]), "no DSN scheme"),
        (kv_with("Key Value", KV.schemes), "invalid adapter kind"),
        (kv_with("kv", BAD), "invalid DSN scheme"),
    ];
    for (adapter, expected) in cases {
        let mut registry = Registry::new();
        let err: RegistryError = registry.register(adapter).unwrap_err();
        assert!(err.to_string().contains(expected), "{err} vs {expected}");
        assert_eq!(
            registry.adapters().len(),
            1,
            "a refused adapter left a trace"
        );
    }
}
