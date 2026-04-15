-- Create cities table to store the list of cities to monitor
CREATE TABLE IF NOT EXISTS cities (
    id SERIAL PRIMARY KEY,
    city_name VARCHAR(255) UNIQUE NOT NULL,
    country_code VARCHAR(10),
    active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert initial Indian cities
INSERT INTO cities (city_name, country_code) VALUES
    ('Ahmedabad', 'IN'),
    ('Mumbai', 'IN'),
    ('Delhi', 'IN'),
    ('Bengaluru', 'IN'),
    ('Chennai', 'IN')
ON CONFLICT (city_name) DO NOTHING;
