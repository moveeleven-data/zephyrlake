### Date vs. DateTime in OpenAQ

OpenAQ runs thousands of sensors worldwide from Los Angeles to Lagos.

When you pass a plain date like `--since 2025-08-01`, the API interprets it as midnight in the sensor’s local timezone.  
That same string points to different UTC instants depending on location:

- New York (UTC-4, summer): `2025-08-01` → `2025-08-01 04:00Z`  
- Tokyo (UTC+9): `2025-08-01` → `2025-07-31 15:00Z`

This leads to inconsistencies: drift across timezones and misaligned UTC partitions.

To avoid this, Zephyrlake always converts a plain date into an explicit UTC instant at midnight:

> 2025-08-01 → 2025-08-01T00:00:00Z

With UTC datetimes, every run slices data along the same universal timeline, ensuring UTC-based partitions regardless of where the sensor is located.