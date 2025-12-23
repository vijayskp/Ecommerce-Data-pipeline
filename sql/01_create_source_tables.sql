CREATE TABLE customers (
    customer_id      INT PRIMARY KEY,
    first_name       VARCHAR(50),
    last_name        VARCHAR(50),
    email            VARCHAR(100) UNIQUE,
    signup_date      DATETIME,
    country          VARCHAR(50),
    city             VARCHAR(50),
    status           VARCHAR(20),  -- e.g. active, inactive
    updated_at       DATETIME
);

CREATE TABLE products (
    product_id       INT PRIMARY KEY,
    product_name     VARCHAR(100),
    category         VARCHAR(50),
    price            DECIMAL(10,2),
    currency         VARCHAR(3),
    updated_at       DATETIME
);

CREATE TABLE orders (
    order_id         INT PRIMARY KEY,
    customer_id      INT,
    order_date       DATETIME,
    order_status     VARCHAR(20),
    total_amount     DECIMAL(10,2),
    currency         VARCHAR(3),
    updated_at       DATETIME,
    CONSTRAINT fk_orders_customers
      FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

CREATE TABLE order_items (
    order_item_id    INT PRIMARY KEY,
    order_id         INT,
    product_id       INT,
    quantity         INT,
    unit_price       DECIMAL(10,2),
    currency         VARCHAR(3),
    updated_at       DATETIME,
    CONSTRAINT fk_order_items_orders
      FOREIGN KEY (order_id) REFERENCES orders(order_id),
    CONSTRAINT fk_order_items_products
      FOREIGN KEY (product_id) REFERENCES products(product_id)
);
