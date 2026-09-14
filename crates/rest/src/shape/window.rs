// Project:   dfe-fetcher
// File:      crates/rest/src/shape/window.rs
// Purpose:   Splitting a tick's window into the steps a unit requests
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Window steps.
//!
//! An incremental unit fetches the scheduler's `[start, end)`; a profile with
//! `window.step` asks for it in chunks of at most that length, each its own
//! page sequence (the m365 24 h chunking). A dump unit has one step and no
//! window. When the scheduler passes no window the profile's `lookback` sets
//! one ending now.

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use dfe_fetcher_core::{FetchWindow, UnitShape};
use serde_json::Value;

use crate::profile::WindowSpec;

/// One request window, pre-formatted for the profile's templates.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Step {
    /// The window this step covers, when the unit is incremental.
    pub window: Option<FetchWindow>,
    /// What templates read as `window`.
    pub json: Value,
}

/// The steps a unit fetches for one tick.
#[must_use]
pub fn steps(
    spec: &WindowSpec,
    shape: UnitShape,
    window: Option<&FetchWindow>,
    now: DateTime<Utc>,
) -> Vec<Step> {
    if shape == UnitShape::Dump {
        return vec![Step {
            window: None,
            json: Value::Null,
        }];
    }
    let window = window.cloned().unwrap_or_else(|| {
        let lookback =
            ChronoDuration::from_std(spec.lookback.0).unwrap_or(ChronoDuration::hours(1));
        FetchWindow {
            start: now - lookback,
            end: now,
        }
    });
    let step = spec
        .step
        .and_then(|d| ChronoDuration::from_std(d.0).ok())
        .filter(|d| *d > ChronoDuration::zero());
    let mut out = Vec::new();
    let mut start = window.start;
    loop {
        let end = match step {
            Some(step) if start + step < window.end => start + step,
            _ => window.end,
        };
        out.push(Step {
            window: Some(FetchWindow { start, end }),
            json: serde_json::json!({
                "start": spec.format.format(start),
                "end": spec.format.format(end),
            }),
        });
        if end >= window.end {
            break;
        }
        start = end;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::profile::{DurationText, WindowFormat};
    use chrono::TimeZone;

    fn at(secs: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(secs, 0).single().unwrap()
    }

    #[test]
    fn a_dump_has_one_step_and_no_window() {
        let s = steps(&WindowSpec::default(), UnitShape::Dump, None, at(0));
        assert_eq!(s.len(), 1);
        assert!(s[0].window.is_none());
        assert_eq!(s[0].json, Value::Null);
    }

    #[test]
    fn without_a_step_the_window_is_one_request_formatted_per_the_profile() {
        let spec = WindowSpec {
            format: WindowFormat::EpochSecs,
            ..WindowSpec::default()
        };
        let window = FetchWindow {
            start: at(100),
            end: at(200),
        };
        let s = steps(&spec, UnitShape::Incremental, Some(&window), at(999));
        assert_eq!(s.len(), 1);
        assert_eq!(s[0].json["start"], "100");
        assert_eq!(s[0].json["end"], "200");
    }

    #[test]
    fn a_step_chunks_the_window_with_a_short_last_chunk() {
        let spec = WindowSpec {
            format: WindowFormat::EpochSecs,
            step: Some(DurationText(std::time::Duration::from_secs(60))),
            ..WindowSpec::default()
        };
        let window = FetchWindow {
            start: at(0),
            end: at(150),
        };
        let s = steps(&spec, UnitShape::Incremental, Some(&window), at(999));
        let bounds: Vec<(String, String)> = s
            .iter()
            .map(|s| {
                (
                    s.json["start"].as_str().unwrap().into(),
                    s.json["end"].as_str().unwrap().into(),
                )
            })
            .collect();
        assert_eq!(
            bounds,
            [
                ("0".into(), "60".into()),
                ("60".into(), "120".into()),
                ("120".into(), "150".into())
            ]
        );
        assert_eq!(s[2].window.as_ref().unwrap().end, at(150));
    }

    #[test]
    fn no_window_means_lookback_ending_now() {
        let spec = WindowSpec {
            format: WindowFormat::EpochSecs,
            lookback: DurationText(std::time::Duration::from_secs(300)),
            ..WindowSpec::default()
        };
        let s = steps(&spec, UnitShape::Incremental, None, at(1000));
        assert_eq!(s[0].json["start"], "700");
        assert_eq!(s[0].json["end"], "1000");
    }
}
