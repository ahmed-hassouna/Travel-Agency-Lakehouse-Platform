-- Create new schema
CREATE SCHEMA IF NOT EXISTS iti;

-- Table: aircrafts
CREATE TABLE iti.aircrafts (
    aircraft_name VARCHAR(255),
    aircraft_iata VARCHAR(10),
    aircraft_icao VARCHAR(10),
    aircraft_id VARCHAR(50),
    PRIMARY KEY (aircraft_id)
);

-- Table: airlines
CREATE TABLE iti.airlines (
    airline_id integer NOT NULL,
    airline_name VARCHAR(100),
    country VARCHAR(50),
    airline_iata VARCHAR(2),        
    airline_icao VARCHAR(3),          
    fleet_size integer,             
    alias VARCHAR(50),
    CONSTRAINT airlines_pkey PRIMARY KEY (airline_id)
);

-- Table: airports
CREATE TABLE iti.airports (
    airport_id   INTEGER PRIMARY KEY,
    airport_name VARCHAR(255),
    city         VARCHAR(150),
    latitude     DOUBLE PRECISION,
    longitude    DOUBLE PRECISION,
    airport_type VARCHAR(20)
);

-- Table: customers
CREATE TABLE iti.customers (
    id INTEGER NOT NULL,
    first_name VARCHAR(100),
    family_name VARCHAR(100),
    gender CHAR(1) CHECK (gender IN ('M', 'F')),
    birth_date DATE,
    country VARCHAR(100),
    phone_number VARCHAR(50),     
    email VARCHAR(255),
    CONSTRAINT customers_pkey PRIMARY KEY (id)
);

-- Table: flight_bookings
CREATE TABLE iti.flight_bookings (
    -- Columns reordered to match the desired INSERT statement order
    trip_id INTEGER NOT NULL,          -- PK Part 1
    customer_id INTEGER, 
    flight_number VARCHAR(32) NOT NULL, -- PK Part 2
    airline_id INTEGER, 
    aircraft_id VARCHAR(32),
    airport_src INTEGER, 
    airport_dst INTEGER,
    departure_time VARCHAR(8), 
    departure_date DATE NOT NULL,      -- PK Part 3
    booking_time VARCHAR(8), 
    booking_date DATE, 
    flight_duration VARCHAR(32),       
    travel_class VARCHAR(32), 
    seat_number VARCHAR(32), 
    price INTEGER, 
    arrival_date DATE, 
    arrival_time VARCHAR(8), 
    payment_method VARCHAR(50), 
    booking_status VARCHAR(20), 
    discount_amount NUMERIC(10,2), 
    final_ticket_price NUMERIC(10,2),
    -- Primary key constraint remains the same
    PRIMARY KEY (trip_id, flight_number, departure_date),
    FOREIGN KEY (customer_id) REFERENCES iti.customers(customer_id),
    FOREIGN KEY (airline_id) REFERENCES iti.airlines(airline_id),
    FOREIGN KEY (aircraft_id) REFERENCES iti.aircrafts(aircraft_id),
    FOREIGN KEY (airport_src) REFERENCES iti.airports(airport_id),
    FOREIGN KEY (airport_dst) REFERENCES iti.airports(airport_id)
);

-- Table: hotel
CREATE TABLE iti.hotel (
    hotel_id INTEGER NOT NULL,
    hotel_name VARCHAR(255),
    hotel_address TEXT,           
    city VARCHAR(100),
    country VARCHAR(100),
    hotel_score DOUBLE PRECISION, 
    star_rating DOUBLE PRECISION,
    room_count INTEGER,
    CONSTRAINT hotel_pk PRIMARY KEY(hotel_id)
);

-- Table: hotel_bookings
CREATE TABLE iti.hotel_bookings(
    -- Primary Key Components (reordered to match logical flow)
    trip_id INTEGER,                      -- FK to trips, nullable
    customer_id INTEGER NOT NULL,         -- PK Part 1
    hotel_id INTEGER NOT NULL,            -- PK Part 2
    check_in_date DATE NOT NULL,          -- PK Part 3
    
    -- Booking Information
    booking_date DATE,
    booking_time VARCHAR(8),              -- Format: HH:MM:SS
    check_out_date DATE,
    
    -- Financial Information
    price NUMERIC(10,2),
    
    -- Additional Attributes
    breakfast_included BOOLEAN,
    payment_method VARCHAR(50),
    payment_status VARCHAR(20),
    
    -- Primary Key (excluding trip_id as requested)
    PRIMARY KEY (customer_id, hotel_id, check_in_date),
    FOREIGN KEY (customer_id) REFERENCES iti.customers(customer_id),
    FOREIGN KEY (hotel_id) REFERENCES iti.hotel(hotel_id)
);