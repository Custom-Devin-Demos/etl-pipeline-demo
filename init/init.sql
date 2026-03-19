-- init/init.sql

CREATE TABLE dealerships (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    region TEXT NOT NULL
);

CREATE TABLE vehicles (
    vin TEXT PRIMARY KEY,
    model TEXT NOT NULL,
    year INT NOT NULL,
    dealership_id INT REFERENCES dealerships(id)
);

CREATE TABLE sales_transactions (
    id SERIAL PRIMARY KEY,
    vin TEXT REFERENCES vehicles(vin),
    sale_date DATE,
    sale_price NUMERIC,
    buyer_name TEXT
);

CREATE TABLE service_records (
    id SERIAL PRIMARY KEY,
    vin TEXT REFERENCES vehicles(vin),
    service_date DATE,
    service_type TEXT,
    service_cost NUMERIC
);

-- Seed some data

INSERT INTO dealerships (name, region) VALUES
('Bay Area Motors', 'West'),
('Midwest Auto Hub', 'Central'),
('Atlantic Car Group', 'East');

INSERT INTO vehicles (vin, model, year, dealership_id) VALUES
('1HGCM82633A004352', 'Camry', 2021, 1),
('1HGCM82633A004353', 'Corolla', 2022, 1),
('1HGCM82633A004354', 'F-150', 2023, 2),
('1HGCM82633A004355', 'Civic', 2022, 3);

INSERT INTO sales_transactions (vin, sale_date, sale_price, buyer_name) VALUES
('1HGCM82633A004352', '2022-01-15', 28000, 'Alice Johnson'),
('1HGCM82633A004353', '2023-03-20', 22000, 'Bob Smith'),
('1HGCM82633A004354', '2024-06-10', 45000, 'Carlos Vega');

INSERT INTO service_records (vin, service_date, service_type, service_cost) VALUES
('1HGCM82633A004352', '2023-02-01', 'Oil Change', 100),
('1HGCM82633A004352', '2024-01-10', 'Brake Pads', 400),
('1HGCM82633A004354', '2025-04-25', 'Transmission Flush', 1200);
