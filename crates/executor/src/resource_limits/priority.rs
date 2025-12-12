use yachtsql_common::error::{Error, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum QueryPriority {
    Low = 0,
    Normal = 1,
    High = 2,
    Critical = 3,
}

impl Default for QueryPriority {
    fn default() -> Self {
        QueryPriority::Normal
    }
}

impl QueryPriority {
    pub fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "low" => Ok(QueryPriority::Low),
            "normal" => Ok(QueryPriority::Normal),
            "high" => Ok(QueryPriority::High),
            "critical" => Ok(QueryPriority::Critical),
            _ => Err(Error::InvalidQuery(format!(
                "Invalid priority level: '{}'. Valid values: low, normal, high, critical",
                s
            ))),
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            QueryPriority::Low => "low",
            QueryPriority::Normal => "normal",
            QueryPriority::High => "high",
            QueryPriority::Critical => "critical",
        }
    }

    pub fn value(&self) -> u8 {
        *self as u8
    }
}

#[derive(Debug, Clone)]
pub struct PriorityConfig {
    pub critical_percent: f32,
    pub high_percent: f32,
    pub normal_percent: f32,
    pub low_percent: f32,
    pub allow_borrowing: bool,
}

impl Default for PriorityConfig {
    fn default() -> Self {
        Self {
            critical_percent: 0.40,
            high_percent: 0.30,
            normal_percent: 0.20,
            low_percent: 0.10,
            allow_borrowing: true,
        }
    }
}

impl PriorityConfig {
    pub fn new(critical: f32, high: f32, normal: f32, low: f32) -> Result<Self> {
        let total = critical + high + normal + low;

        if (total - 1.0).abs() > 0.001 {
            return Err(Error::InvalidQuery(format!(
                "Priority percentages must sum to 1.0 (100%), got {:.3}",
                total
            )));
        }

        if critical < 0.0 || high < 0.0 || normal < 0.0 || low < 0.0 {
            return Err(Error::InvalidQuery(
                "Priority percentages must be non-negative".to_string(),
            ));
        }

        Ok(Self {
            critical_percent: critical,
            high_percent: high,
            normal_percent: normal,
            low_percent: low,
            allow_borrowing: true,
        })
    }

    pub fn balanced() -> Self {
        Self {
            critical_percent: 0.25,
            high_percent: 0.25,
            normal_percent: 0.25,
            low_percent: 0.25,
            allow_borrowing: true,
        }
    }

    pub fn high_priority_focused() -> Self {
        Self {
            critical_percent: 0.50,
            high_percent: 0.35,
            normal_percent: 0.10,
            low_percent: 0.05,
            allow_borrowing: true,
        }
    }

    pub fn with_borrowing(mut self, allow: bool) -> Self {
        self.allow_borrowing = allow;
        self
    }

    pub fn get_percent(&self, priority: QueryPriority) -> f32 {
        match priority {
            QueryPriority::Critical => self.critical_percent,
            QueryPriority::High => self.high_percent,
            QueryPriority::Normal => self.normal_percent,
            QueryPriority::Low => self.low_percent,
        }
    }

    pub fn tier_limit(&self, priority: QueryPriority, total_budget: usize) -> usize {
        let percent = self.get_percent(priority);
        (total_budget as f64 * percent as f64) as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_priority_ordering() {
        assert!(QueryPriority::Critical > QueryPriority::High);
        assert!(QueryPriority::High > QueryPriority::Normal);
        assert!(QueryPriority::Normal > QueryPriority::Low);
    }

    #[test]
    fn test_priority_from_str() {
        assert_eq!(QueryPriority::from_str("low").unwrap(), QueryPriority::Low);
        assert_eq!(
            QueryPriority::from_str("NORMAL").unwrap(),
            QueryPriority::Normal
        );
        assert_eq!(
            QueryPriority::from_str("High").unwrap(),
            QueryPriority::High
        );
        assert_eq!(
            QueryPriority::from_str("CRITICAL").unwrap(),
            QueryPriority::Critical
        );

        assert!(QueryPriority::from_str("invalid").is_err());
    }

    #[test]
    fn test_priority_default() {
        assert_eq!(QueryPriority::default(), QueryPriority::Normal);
    }

    #[test]
    fn test_priority_config_default() {
        let config = PriorityConfig::default();
        let total = config.critical_percent
            + config.high_percent
            + config.normal_percent
            + config.low_percent;
        assert!((total - 1.0).abs() < 0.001);
    }

    #[test]
    fn test_priority_config_new() {
        let config = PriorityConfig::new(0.5, 0.3, 0.15, 0.05).unwrap();
        assert_eq!(config.critical_percent, 0.5);
        assert_eq!(config.high_percent, 0.3);
        assert_eq!(config.normal_percent, 0.15);
        assert_eq!(config.low_percent, 0.05);
    }

    #[test]
    fn test_priority_config_validation() {
        assert!(PriorityConfig::new(0.5, 0.3, 0.1, 0.05).is_err());

        assert!(PriorityConfig::new(0.5, 0.3, 0.2, -0.1).is_err());
    }

    #[test]
    fn test_priority_config_balanced() {
        let config = PriorityConfig::balanced();
        assert_eq!(config.critical_percent, 0.25);
        assert_eq!(config.high_percent, 0.25);
        assert_eq!(config.normal_percent, 0.25);
        assert_eq!(config.low_percent, 0.25);
    }

    #[test]
    fn test_tier_limit() {
        let config = PriorityConfig::default();
        let total = 1000;

        assert_eq!(config.tier_limit(QueryPriority::Critical, total), 400);
        assert_eq!(config.tier_limit(QueryPriority::High, total), 300);
        assert_eq!(config.tier_limit(QueryPriority::Normal, total), 200);
        assert_eq!(config.tier_limit(QueryPriority::Low, total), 100);
    }
}
