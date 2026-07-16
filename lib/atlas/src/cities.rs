//! A compact table of real cities the display is allowed to show. Client
//! locations are always snapped to the nearest entry, so a dot on the globe
//! never means anything more precise than a major city.

use std::sync::OnceLock;

/// One displayable city.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct City {
    pub name: &'static str,
    pub cc: &'static str,
    pub lat: f32,
    pub lon: f32,
}

const fn city(name: &'static str, cc: &'static str, lat: f32, lon: f32) -> City {
    City { name, cc, lat, lon }
}

/// Per-city trigonometry for the nearest scan, computed once. The scan runs
/// for every user event, so the table's sin/cos must not be redone each call.
struct CityTrig {
    sin_lat: f32,
    cos_lat: f32,
    lon: f32,
}

fn trig_table() -> &'static [CityTrig] {
    static TABLE: OnceLock<Vec<CityTrig>> = OnceLock::new();
    TABLE.get_or_init(|| {
        CITIES
            .iter()
            .map(|c| {
                let (sin_lat, cos_lat) = c.lat.to_radians().sin_cos();
                CityTrig { sin_lat, cos_lat, lon: c.lon.to_radians() }
            })
            .collect()
    })
}

/// Index of the city nearest to a coordinate, by great-circle distance.
pub fn nearest_idx(lat: f32, lon: f32) -> usize {
    let (sp, cp) = lat.to_radians().sin_cos();
    let plon = lon.to_radians();
    let mut best = 0;
    let mut best_score = f32::MIN;
    for (i, c) in trig_table().iter().enumerate() {
        // cosine of the angular distance; bigger is closer
        let score = sp * c.sin_lat + cp * c.cos_lat * (c.lon - plon).cos();
        if score > best_score {
            best_score = score;
            best = i;
        }
    }
    best
}

/// The city nearest to a coordinate.
pub fn nearest(lat: f32, lon: f32) -> &'static City {
    &CITIES[nearest_idx(lat, lon)]
}

/// Every city the display may name. Ordering carries meaning: the first entry
/// for a country is its representative, where country-only geoip matches land.
pub const CITIES: &[City] = &[
    // North America
    city("New York", "US", 40.71, -74.01),
    city("Los Angeles", "US", 34.05, -118.24),
    city("Chicago", "US", 41.88, -87.63),
    city("Houston", "US", 29.76, -95.37),
    city("Phoenix", "US", 33.45, -112.07),
    city("Dallas", "US", 32.78, -96.80),
    city("San Francisco", "US", 37.77, -122.42),
    city("San Jose", "US", 37.34, -121.89),
    city("Seattle", "US", 47.61, -122.33),
    city("Denver", "US", 39.74, -104.99),
    city("Boston", "US", 42.36, -71.06),
    city("Atlanta", "US", 33.75, -84.39),
    city("Miami", "US", 25.76, -80.19),
    city("Washington", "US", 38.91, -77.04),
    city("Ashburn", "US", 39.04, -77.49),
    city("Philadelphia", "US", 39.95, -75.17),
    city("Minneapolis", "US", 44.98, -93.27),
    city("Salt Lake City", "US", 40.76, -111.89),
    city("Portland", "US", 45.52, -122.68),
    city("Las Vegas", "US", 36.17, -115.14),
    city("San Diego", "US", 32.72, -117.16),
    city("Austin", "US", 30.27, -97.74),
    city("Kansas City", "US", 39.10, -94.58),
    city("St. Louis", "US", 38.63, -90.20),
    city("Charlotte", "US", 35.23, -80.84),
    city("Columbus", "US", 39.96, -83.00),
    city("Detroit", "US", 42.33, -83.05),
    city("Anchorage", "US", 61.22, -149.90),
    city("Honolulu", "US", 21.31, -157.86),
    city("Toronto", "CA", 43.65, -79.38),
    city("Montreal", "CA", 45.50, -73.57),
    city("Vancouver", "CA", 49.28, -123.12),
    city("Calgary", "CA", 51.05, -114.07),
    city("Ottawa", "CA", 45.42, -75.70),
    city("Mexico City", "MX", 19.43, -99.13),
    city("Guadalajara", "MX", 20.67, -103.35),
    city("Monterrey", "MX", 25.67, -100.31),
    // Latin America
    city("Sao Paulo", "BR", -23.55, -46.63),
    city("Rio de Janeiro", "BR", -22.91, -43.17),
    city("Brasilia", "BR", -15.79, -47.88),
    city("Fortaleza", "BR", -3.72, -38.54),
    city("Porto Alegre", "BR", -30.03, -51.23),
    city("Buenos Aires", "AR", -34.60, -58.38),
    city("Santiago", "CL", -33.45, -70.67),
    city("Lima", "PE", -12.05, -77.04),
    city("Bogota", "CO", 4.71, -74.07),
    city("Medellin", "CO", 6.24, -75.58),
    city("Quito", "EC", -0.18, -78.47),
    city("Caracas", "VE", 10.49, -66.88),
    city("Montevideo", "UY", -34.90, -56.16),
    city("Asuncion", "PY", -25.26, -57.58),
    city("La Paz", "BO", -16.49, -68.15),
    city("Panama City", "PA", 8.98, -79.52),
    city("San Jose", "CR", 9.93, -84.08),
    city("Guatemala City", "GT", 14.63, -90.51),
    city("Santo Domingo", "DO", 18.49, -69.93),
    city("San Juan", "PR", 18.47, -66.11),
    city("Havana", "CU", 23.11, -82.37),
    city("Kingston", "JM", 18.02, -76.80),
    // Europe
    city("London", "GB", 51.51, -0.13),
    city("Manchester", "GB", 53.48, -2.24),
    city("Edinburgh", "GB", 55.95, -3.19),
    city("Dublin", "IE", 53.35, -6.26),
    city("Paris", "FR", 48.86, 2.35),
    city("Marseille", "FR", 43.30, 5.37),
    city("Lyon", "FR", 45.76, 4.84),
    city("Madrid", "ES", 40.42, -3.70),
    city("Barcelona", "ES", 41.39, 2.17),
    city("Lisbon", "PT", 38.72, -9.14),
    city("Berlin", "DE", 52.52, 13.40),
    city("Frankfurt", "DE", 50.11, 8.68),
    city("Munich", "DE", 48.14, 11.58),
    city("Hamburg", "DE", 53.55, 9.99),
    city("Falkenstein", "DE", 50.48, 12.37),
    city("Dusseldorf", "DE", 51.23, 6.78),
    city("Amsterdam", "NL", 52.37, 4.90),
    city("Rotterdam", "NL", 51.92, 4.48),
    city("Brussels", "BE", 50.85, 4.35),
    city("Zurich", "CH", 47.38, 8.54),
    city("Geneva", "CH", 46.20, 6.15),
    city("Vienna", "AT", 48.21, 16.37),
    city("Prague", "CZ", 50.08, 14.44),
    city("Warsaw", "PL", 52.23, 21.01),
    city("Krakow", "PL", 50.06, 19.94),
    city("Budapest", "HU", 47.50, 19.04),
    city("Bucharest", "RO", 44.43, 26.10),
    city("Sofia", "BG", 42.70, 23.32),
    city("Athens", "GR", 37.98, 23.73),
    city("Rome", "IT", 41.90, 12.50),
    city("Milan", "IT", 45.46, 9.19),
    city("Naples", "IT", 40.85, 14.27),
    city("Copenhagen", "DK", 55.68, 12.57),
    city("Stockholm", "SE", 59.33, 18.07),
    city("Gothenburg", "SE", 57.71, 11.97),
    city("Oslo", "NO", 59.91, 10.75),
    city("Helsinki", "FI", 60.17, 24.94),
    city("Tallinn", "EE", 59.44, 24.75),
    city("Riga", "LV", 56.95, 24.11),
    city("Vilnius", "LT", 54.69, 25.28),
    city("Kyiv", "UA", 50.45, 30.52),
    city("Istanbul", "TR", 41.01, 28.98),
    city("Ankara", "TR", 39.93, 32.86),
    city("Belgrade", "RS", 44.79, 20.45),
    city("Zagreb", "HR", 45.81, 15.98),
    city("Luxembourg", "LU", 49.61, 6.13),
    city("Reykjavik", "IS", 64.15, -21.94),
    city("Moscow", "RU", 55.76, 37.62),
    city("St. Petersburg", "RU", 59.93, 30.34),
    // Africa
    city("Cairo", "EG", 30.04, 31.24),
    city("Alexandria", "EG", 31.20, 29.92),
    city("Lagos", "NG", 6.52, 3.38),
    city("Abuja", "NG", 9.06, 7.49),
    city("Accra", "GH", 5.60, -0.19),
    city("Abidjan", "CI", 5.36, -4.01),
    city("Dakar", "SN", 14.72, -17.47),
    city("Casablanca", "MA", 33.57, -7.59),
    city("Algiers", "DZ", 36.75, 3.06),
    city("Tunis", "TN", 36.81, 10.18),
    city("Nairobi", "KE", -1.29, 36.82),
    city("Addis Ababa", "ET", 9.01, 38.75),
    city("Dar es Salaam", "TZ", -6.79, 39.21),
    city("Kampala", "UG", 0.35, 32.58),
    city("Kigali", "RW", -1.95, 30.06),
    city("Johannesburg", "ZA", -26.20, 28.05),
    city("Cape Town", "ZA", -33.92, 18.42),
    city("Durban", "ZA", -29.86, 31.03),
    city("Luanda", "AO", -8.84, 13.23),
    city("Kinshasa", "CD", -4.44, 15.27),
    city("Lusaka", "ZM", -15.39, 28.32),
    city("Harare", "ZW", -17.83, 31.05),
    city("Maputo", "MZ", -25.97, 32.57),
    // Middle East
    city("Dubai", "AE", 25.20, 55.27),
    city("Abu Dhabi", "AE", 24.45, 54.38),
    city("Doha", "QA", 25.29, 51.53),
    city("Riyadh", "SA", 24.71, 46.68),
    city("Jeddah", "SA", 21.49, 39.19),
    city("Kuwait City", "KW", 29.38, 47.99),
    city("Manama", "BH", 26.23, 50.59),
    city("Muscat", "OM", 23.59, 58.41),
    city("Tel Aviv", "IL", 32.09, 34.78),
    city("Amman", "JO", 31.96, 35.95),
    city("Beirut", "LB", 33.89, 35.50),
    city("Baghdad", "IQ", 33.31, 44.37),
    city("Tehran", "IR", 35.69, 51.39),
    // Asia
    city("Tokyo", "JP", 35.68, 139.76),
    city("Osaka", "JP", 34.69, 135.50),
    city("Nagoya", "JP", 35.18, 136.91),
    city("Fukuoka", "JP", 33.59, 130.40),
    city("Sapporo", "JP", 43.06, 141.35),
    city("Seoul", "KR", 37.57, 126.98),
    city("Busan", "KR", 35.18, 129.08),
    city("Beijing", "CN", 39.90, 116.41),
    city("Shanghai", "CN", 31.23, 121.47),
    city("Shenzhen", "CN", 22.54, 114.06),
    city("Guangzhou", "CN", 23.13, 113.26),
    city("Chengdu", "CN", 30.57, 104.07),
    city("Hangzhou", "CN", 30.27, 120.16),
    city("Hong Kong", "HK", 22.32, 114.17),
    city("Taipei", "TW", 25.03, 121.57),
    city("Manila", "PH", 14.60, 120.98),
    city("Cebu", "PH", 10.32, 123.90),
    city("Bangkok", "TH", 13.76, 100.50),
    city("Ho Chi Minh City", "VN", 10.82, 106.63),
    city("Hanoi", "VN", 21.03, 105.85),
    city("Phnom Penh", "KH", 11.56, 104.92),
    city("Singapore", "SG", 1.35, 103.82),
    city("Kuala Lumpur", "MY", 3.14, 101.69),
    city("Jakarta", "ID", -6.21, 106.85),
    city("Surabaya", "ID", -7.26, 112.75),
    city("Mumbai", "IN", 19.08, 72.88),
    city("Delhi", "IN", 28.61, 77.21),
    city("Bengaluru", "IN", 12.97, 77.59),
    city("Hyderabad", "IN", 17.39, 78.49),
    city("Chennai", "IN", 13.08, 80.27),
    city("Kolkata", "IN", 22.57, 88.36),
    city("Pune", "IN", 18.52, 73.86),
    city("Karachi", "PK", 24.86, 67.01),
    city("Lahore", "PK", 31.55, 74.34),
    city("Islamabad", "PK", 33.68, 73.05),
    city("Dhaka", "BD", 23.81, 90.41),
    city("Colombo", "LK", 6.93, 79.85),
    city("Kathmandu", "NP", 27.72, 85.32),
    city("Almaty", "KZ", 43.24, 76.89),
    city("Tashkent", "UZ", 41.30, 69.24),
    city("Baku", "AZ", 40.41, 49.87),
    city("Tbilisi", "GE", 41.72, 44.79),
    city("Yerevan", "AM", 40.18, 44.51),
    // Oceania
    city("Sydney", "AU", -33.87, 151.21),
    city("Melbourne", "AU", -37.81, 144.96),
    city("Brisbane", "AU", -27.47, 153.03),
    city("Perth", "AU", -31.95, 115.86),
    city("Adelaide", "AU", -34.93, 138.60),
    city("Auckland", "NZ", -36.85, 174.76),
    city("Wellington", "NZ", -41.29, 174.78),
];

#[cfg(test)]
mod tests {
    use super::*;

    // coordinates snap to the nearest listed city
    #[test]
    fn nearest_city() {
        assert_eq!(nearest(48.80, 2.40).name, "Paris");
        assert_eq!(nearest(50.5, 12.4).name, "Falkenstein");
        assert_eq!(nearest(-33.9, 151.0).name, "Sydney");
        assert_eq!(nearest(1.0, 104.0).name, "Singapore");
    }

    // a point just past the date line still finds the pacific side
    #[test]
    fn antimeridian() {
        let city = nearest(-38.0, -179.0);

        assert!(city.cc == "NZ", "got {}", city.name);
    }

    // country-only geoip matches land on the first entry, so each country
    // must lead with its flagship city
    #[test]
    fn country_representatives() {
        let first = |cc: &str| CITIES.iter().find(|c| c.cc == cc).map(|c| c.name);

        assert_eq!(first("US"), Some("New York"));
        assert_eq!(first("DE"), Some("Berlin"));
        assert_eq!(first("IT"), Some("Rome"));
        assert_eq!(first("JP"), Some("Tokyo"));
    }
}
