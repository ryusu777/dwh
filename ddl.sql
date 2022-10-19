create table dim_customer (
    customer_id varchar(12),
    name varchar(50),
    dob date,
    phone varchar(20),
    address varchar(255),
    primary key (customer_id)
);

create table dim_drug (
    drug_id varchar(12),
    name varchar(255),
    category varchar(255),
    price_buy float,
    price_sell float,
    expired date,
    primary key (drug_id)
);

create table dim_date (
    date_id serial,
    date date,
    month int,
    year int,
    primary key (date_id)
);

create table dim_drug_store (
    drug_store_id varchar(12),
    location varchar(255),
    name varchar(50),
    primary key (drug_store_id)
);

create table dim_pharmacist (
    pharmacist_id varchar(12),
    name varchar(50),
    phone varchar(20),
    dob date,
    address varchar(255),
    salary float,
    primary key (pharmacist_id)
);

create table fact_recap_pharmacy (
    recap_pharmacy_id serial,
    customer_id varchar(12),
    drug_id varchar(12),
    date_id int,
    drug_store_id varchar(12),
    pharmacist_id varchar(12),
    revenue float,
    expense float,
    income float,
    total_sales int,
    foreign key (customer_id) references dim_customer(customer_id),
    foreign key (drug_id) references dim_drug(drug_id),
    foreign key (date_id) references dim_date(date_id),
    foreign key (drug_store_id) references dim_drug_store(drug_store_id),
    foreign key (pharmacist_id) references dim_pharmacist(pharmacist_id)
);
