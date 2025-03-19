CREATE TABLE users
(
    id         SERIAL PRIMARY KEY,
    name       TEXT,
    email      TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

-- CREATE TABLE orders
-- (
--     id         SERIAL PRIMARY KEY,
--     user_id    INT,
--     amount     DECIMAL(10, 2),
--     created_at TIMESTAMP DEFAULT NOW()
-- );



INSERT INTO users (name, email, created_at)
SELECT 'User_' || generate_series,
       'user' || generate_series || '@example.com',
       NOW() - (random() * interval '5 years')
FROM generate_series(1, 5000000);


-- INSERT INTO orders (user_id, amount, created_at)
-- SELECT floor(random() * 5000000) + 1,
--        round((random() * 1000 + 10)::numeric, 2),
--        NOW() - (random() * interval '5 years')
-- FROM generate_series(1, 5000000);
