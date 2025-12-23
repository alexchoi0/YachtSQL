use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
pub enum WeekStartDay {
    #[default]
    Sunday,
    Monday,
    Tuesday,
    Wednesday,
    Thursday,
    Friday,
    Saturday,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DateTimeField {
    Year,
    IsoYear,
    Quarter,
    Month,
    Week(WeekStartDay),
    IsoWeek,
    Day,
    DayOfWeek,
    DayOfYear,
    Hour,
    Minute,
    Second,
    Millisecond,
    Microsecond,
    Nanosecond,
    Date,
    Time,
    Datetime,
    Timezone,
    TimezoneHour,
    TimezoneMinute,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
pub enum TrimWhere {
    #[default]
    Both,
    Leading,
    Trailing,
}
