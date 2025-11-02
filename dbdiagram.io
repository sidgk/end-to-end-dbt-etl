// ==========================
// DIMENSION TABLES
// ==========================



Table dim_partners {
  partner_id varchar [pk]
  partner_name varchar
  partner_type varchar
  country varchar
  createdAt timestamp
  dwh_load_time timestamp
}

Table dim_tickets {
  ticket_id varchar [pk]
  booking_id varchar [ref: > fact_bookings.booking_id]
  bookingPrice numeric
  bookingCurrency varchar
  vendorCode varchar
}

Table dim_passengers {
  passenger_id varchar [pk]
  booking_id varchar [ref: > fact_bookings.booking_id]
  first_name varchar
  last_name varchar
  age int
  passenger_type varchar
  createdAt timestamp
}

Table dim_segments {
  segment_id varchar [pk]
  booking_id varchar [ref: > fact_bookings.booking_id]
  departure_station varchar
  arrival_station varchar
  duration_minutes int
  carrier varchar
}

// ==========================
// FACT TABLE
// ==========================

Table fact_bookings {
  booking_id varchar [pk]
  booking_date date 
  partner_id_offer varchar [ref: > dim_partners.partner_id]
  booking_price numeric
  booking_currency varchar
  booking_status varchar
}

// ==========================
// SNAPSHOTS (Derived tables)
// ==========================

Table booking_status_snapshot {
  booking_id varchar [ref: > fact_bookings.booking_id]
  booking_status varchar
  dbt_valid_from timestamp
  dbt_valid_to timestamp
}

Table passenger_snapshot {
  passenger_id varchar [ref: > dim_passengers.passenger_id]
  first_name varchar
  last_name varchar
  age int
  passenger_type varchar
  dbt_valid_from timestamp
  dbt_valid_to timestamp
}
