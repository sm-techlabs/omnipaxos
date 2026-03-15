//! Simulated clock implementation with drift, uncertainty, and offset modeling.
use rand::Rng;
use std::time::{SystemTime, UNIX_EPOCH};
use std::env;
/// Internal state of a simulated clock used to model uncertainty, drift,
/// frequency skew, and base offset.
///
/// All values are expressed in microseconds unless otherwise noted.
pub struct ClockState {
    /// Maximum absolute random jitter applied to each timestamp (μs).
    uncertainty: i64,

    /// Clock drift rate, applied proportionally to the modulo of the frequency (μs/s).
    drift_rate: i64,

    /// Clock frequency used to model periodic drift behavior (μs).
    frequency: i64,

    /// Fixed base offset applied to all timestamps (μs).
    base_offset: i64,
}

impl ClockState {
    /// Creates a new [`ClockState`] with the given parameters.
    ///
    /// # Parameters
    /// - `uncertainty`: Maximum absolute random jitter in microseconds.
    /// - `drift_rate`: Drift rate applied per second.
    /// - `frequency`: Frequency used to compute drift modulation.
    /// - `base_offset`: Constant offset applied to all timestamps.
    pub fn new(uncertainty: i64, drift_rate: i64, frequency: i64, base_offset: i64) -> Self {
        Self {
            uncertainty,
            drift_rate,
            frequency,
            base_offset,
        }
    }

    /// Creates a Sim Clock from env variables
    pub fn new_from_env() -> Self {
        let default_val: i64 = 0;
        let uncertainty = env::var("CLOCK_UNCERTAINTY").ok().and_then(|s| s.parse::<i64>().ok()).unwrap_or(default_val);
        let drift_rate = env::var("CLOCK_DRIFT").ok().and_then(|s| s.parse::<i64>().ok()).unwrap_or(default_val);
        let frequency = env::var("CLOCK_SYNC_FREQUENCY").ok().and_then(|s| s.parse::<i64>().ok()).unwrap_or(default_val);
        let base_offset = env::var("CLOCK_OFFSET").ok().and_then(|s| s.parse::<i64>().ok()).unwrap_or(default_val);
        Self {
            uncertainty,
            drift_rate,
            frequency,
            base_offset,
        }
    }

    /// Reinitializes the clock state with new parameter values.
    ///
    /// This allows reusing an existing instance without reallocating.
    pub fn init(&mut self, uncertainty: i64, drift_rate: i64, frequency: i64, base_offset: i64) {
        self.uncertainty = uncertainty;
        self.drift_rate = drift_rate;
        self.frequency = frequency;
        self.base_offset = base_offset;
    }

    /// Returns the configured clock uncertainty in microseconds.
    pub fn get_uncertainty(&self) -> i64 {
        self.uncertainty
    }

    /// Computes the current simulated timestamp in microseconds.
    ///
    /// The calculation applies:
    /// - system time
    /// - frequency-based drift
    /// - constant base offset
    /// - bounded random jitter
    ///
    /// # Formula
    /// ```text
    /// now + ((now % frequency) * drift_rate) + base_offset + jitter
    /// ```
    pub fn get_time(&self) -> i64 {
        // 1. Get current system time in microseconds
        let start = SystemTime::now();
        let since_the_epoch = start
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards");

        let now_micros = since_the_epoch.as_micros() as i64;

        // Avoid divide-by-zero when frequency is zero.
        let modulo_val = if self.frequency > 0 {
            now_micros % self.frequency
        } else {
            0
        };

        // Generate random jitter in range [-uncertainty, uncertainty]
        let mut rng = rand::thread_rng();
        let jitter = rng.gen_range(-self.uncertainty..=self.uncertainty);

        now_micros + (modulo_val * self.drift_rate) + self.base_offset + jitter
    }
}
