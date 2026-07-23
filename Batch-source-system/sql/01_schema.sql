-- =========================================================
-- E-commerce OLTP Source Schema
-- This simulates a real production application database
-- =========================================================

CREATE SCHEMA IF NOT EXISTS retail;
SET search_path TO retail;

-- ---------------------------------------------------------
-- CUSTOMERS
-- ---------------------------------------------------------
CREATE TABLE customers (
    customer_id     SERIAL PRIMARY KEY,
    first_name      VARCHAR(100) NOT NULL,
    last_name       VARCHAR(100) NOT NULL,
    email           VARCHAR(255) UNIQUE NOT NULL,
    phone           VARCHAR(30),
    address_line    VARCHAR(255),
    city            VARCHAR(100),
    state           VARCHAR(100),
    country         VARCHAR(100),
    postal_code     VARCHAR(20),
    signup_date     TIMESTAMP NOT NULL DEFAULT now(),
    is_active       BOOLEAN NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMP NOT NULL DEFAULT now(),
    updated_at      TIMESTAMP NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------
-- PRODUCTS
-- ---------------------------------------------------------
CREATE TABLE products (
    product_id      SERIAL PRIMARY KEY,
    product_name    VARCHAR(255) NOT NULL,
    category        VARCHAR(100) NOT NULL,
    sub_category    VARCHAR(100),
    brand           VARCHAR(100),
    price           NUMERIC(10,2) NOT NULL,
    stock_qty       INTEGER NOT NULL DEFAULT 0,
    is_discontinued BOOLEAN NOT NULL DEFAULT FALSE,
    created_at      TIMESTAMP NOT NULL DEFAULT now(),
    updated_at      TIMESTAMP NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------
-- ORDERS
-- ---------------------------------------------------------
CREATE TABLE orders (
    order_id        SERIAL PRIMARY KEY,
    customer_id     INTEGER NOT NULL REFERENCES customers(customer_id),
    order_date      TIMESTAMP NOT NULL DEFAULT now(),
    status          VARCHAR(30) NOT NULL DEFAULT 'PENDING',
        -- PENDING -> CONFIRMED -> SHIPPED -> DELIVERED / CANCELLED
    shipping_city   VARCHAR(100),
    shipping_country VARCHAR(100),
    total_amount    NUMERIC(12,2) NOT NULL DEFAULT 0,
    created_at      TIMESTAMP NOT NULL DEFAULT now(),
    updated_at      TIMESTAMP NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------
-- ORDER ITEMS
-- ---------------------------------------------------------
CREATE TABLE order_items (
    order_item_id   SERIAL PRIMARY KEY,
    order_id        INTEGER NOT NULL REFERENCES orders(order_id),
    product_id      INTEGER NOT NULL REFERENCES products(product_id),
    quantity        INTEGER NOT NULL,
    unit_price      NUMERIC(10,2) NOT NULL,
    created_at      TIMESTAMP NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------
-- PAYMENTS
-- ---------------------------------------------------------
CREATE TABLE payments (
    payment_id      SERIAL PRIMARY KEY,
    order_id        INTEGER NOT NULL REFERENCES orders(order_id),
    amount          NUMERIC(12,2) NOT NULL,
    method          VARCHAR(30) NOT NULL, -- CARD, UPI, NETBANKING, COD
    status          VARCHAR(30) NOT NULL DEFAULT 'INITIATED',
        -- INITIATED -> SUCCESS / FAILED / REFUNDED
    paid_at         TIMESTAMP,
    created_at      TIMESTAMP NOT NULL DEFAULT now(),
    updated_at      TIMESTAMP NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------
-- Indexes (mimic real production tuning)
-- ---------------------------------------------------------
CREATE INDEX idx_orders_customer_id ON orders(customer_id);
CREATE INDEX idx_orders_updated_at ON orders(updated_at);
CREATE INDEX idx_order_items_order_id ON order_items(order_id);
CREATE INDEX idx_payments_order_id ON payments(order_id);
CREATE INDEX idx_customers_updated_at ON customers(updated_at);
CREATE INDEX idx_products_updated_at ON products(updated_at);

-- ---------------------------------------------------------
-- Auto-update "updated_at" on row changes (mimics real apps)
-- ---------------------------------------------------------
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_customers_updated_at
BEFORE UPDATE ON customers
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE TRIGGER trg_products_updated_at
BEFORE UPDATE ON products
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE TRIGGER trg_orders_updated_at
BEFORE UPDATE ON orders
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE TRIGGER trg_payments_updated_at
BEFORE UPDATE ON payments
FOR EACH ROW EXECUTE FUNCTION set_updated_at();
