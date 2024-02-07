-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Case Study #1 - Danny's Diner
-- MAGIC
-- MAGIC ![Image Alt Text](https://8weeksqlchallenge.com/images/case-study-designs/1.png)
-- MAGIC
-- MAGIC # Introduction
-- MAGIC
-- MAGIC Danny seriously loves Japanese food so in the beginning of 2021, he decides to embark upon a risky venture and opens up a cute little restaurant that sells his 3 favourite foods: sushi, curry and ramen.
-- MAGIC
-- MAGIC Danny’s Diner is in need of your assistance to help the restaurant stay afloat - the restaurant has captured some very basic data from their few months of operation but have no idea how to use their data to help them run the business.
-- MAGIC
-- MAGIC # Problem Statement
-- MAGIC
-- MAGIC Danny wants to use the data to answer a few simple questions about his customers, especially about their visiting patterns, how much money they’ve spent and also which menu items are their favourite. Having this deeper connection with his customers will help him deliver a better and more personalised experience for his loyal customers.
-- MAGIC
-- MAGIC He plans on using these insights to help him decide whether he should expand the existing customer loyalty program - additionally he needs help to generate some basic datasets so his team can easily inspect the data without needing to use SQL.
-- MAGIC
-- MAGIC Danny has provided you with a sample of his overall customer data due to privacy issues - but he hopes that these examples are enough for you to write fully functioning SQL queries to help him answer his questions!
-- MAGIC
-- MAGIC Danny has shared with you 3 key datasets for this case study:
-- MAGIC
-- MAGIC 1. ***sales***
-- MAGIC 2. ***menu***
-- MAGIC 3. ***members***
-- MAGIC
-- MAGIC You can inspect the entity relationship diagram and example data below.
-- MAGIC
-- MAGIC [Database Diagram Click to see ERD](https://dbdiagram.io/d/Copy-of-Dannys-Diner-65c25789ac844320ae93b439)
-- MAGIC
-- MAGIC
-- MAGIC # Example Datasets
-- MAGIC
-- MAGIC All datasets exist within the ***dannys_diner*** database schema - be sure to include this reference within your SQL scripts as you start exploring the data and answering the case study questions.
-- MAGIC
-- MAGIC # Table 1: sales
-- MAGIC
-- MAGIC The ***sales*** table captures all ***customer_id*** level purchases with an corresponding ***order_date*** and ***product_id*** information for when and what menu items were ordered.
-- MAGIC
-- MAGIC | customer_id | order_date | product_id |
-- MAGIC |-------------|------------|------------|
-- MAGIC | A           | 2021-01-01 | 1          |
-- MAGIC | A           | 2021-01-01 | 2          |
-- MAGIC | A           | 2021-01-07 | 2          |
-- MAGIC | A           | 2021-01-10 | 3          |
-- MAGIC | A           | 2021-01-11 | 3          |
-- MAGIC | A           | 2021-01-11 | 3          |
-- MAGIC | B           | 2021-01-01 | 2          |
-- MAGIC | B           | 2021-01-02 | 2          |
-- MAGIC | B           | 2021-01-04 | 1          |
-- MAGIC | B           | 2021-01-11 | 1          |
-- MAGIC | B           | 2021-01-16 | 3          |
-- MAGIC | B           | 2021-02-01 | 3          |
-- MAGIC | C           | 2021-01-01 | 3          |
-- MAGIC | C           | 2021-01-01 | 3          |
-- MAGIC | C           | 2021-01-07 | 3          |
-- MAGIC
-- MAGIC
-- MAGIC # Table 2: menu
-- MAGIC
-- MAGIC The ***menu*** table maps the ***product_id*** to the actual ***product_name*** and ***price*** of each menu item.
-- MAGIC
-- MAGIC | product_id | product_name | price |
-- MAGIC |------------|--------------|-------|
-- MAGIC | 1          | sushi        | 10    |
-- MAGIC | 2          | curry        | 15    |
-- MAGIC | 3          | ramen        | 12    |
-- MAGIC
-- MAGIC # Table 3: members
-- MAGIC
-- MAGIC The final ***members*** table captures the ***join_date*** when a ***customer_id*** joined the beta version of the Danny’s Diner loyalty program.
-- MAGIC
-- MAGIC | customer_id | join_date |
-- MAGIC |-------------|-----------|
-- MAGIC | A           | 2021-01-07|
-- MAGIC | B           | 2021-01-09|
-- MAGIC
-- MAGIC

-- COMMAND ----------


-- CREATE SCHEMA dannys_diner; {use this is postgtres}
-- SET search_path = dannys_diner; {use this is postgtres}


CREATE DATABASE IF NOT EXISTS dd
LOCATION 'dbfs:/user/hive/warehouse/dd.db'; --{use this is databricks}
USE dd; --{use this is databricks}

CREATE TABLE IF NOT EXISTS sales (
  customer_id VARCHAR(1),
  order_date DATE,
  product_id INTEGER
);

INSERT INTO sales
  VALUES
  ('A', '2021-01-01', 1),
  ('A', '2021-01-01', 2),
  ('A', '2021-01-07', 2),
  ('A', '2021-01-10', 3),
  ('A', '2021-01-11', 3),
  ('A', '2021-01-11', 3),
  ('B', '2021-01-01', 2),
  ('B', '2021-01-02', 2),
  ('B', '2021-01-04', 1),
  ('B', '2021-01-11', 1),
  ('B', '2021-01-16', 3),
  ('B', '2021-02-01', 3),
  ('C', '2021-01-01', 3),
  ('C', '2021-01-01', 3),
  ('C', '2021-01-07', 3);

CREATE TABLE IF NOT EXISTS menu (
  product_id INTEGER,
  product_name VARCHAR(5),
  price INTEGER
);

INSERT INTO menu
  VALUES
  (1, 'sushi', 10),
  (2, 'curry', 15),
  (3, 'ramen', 12);

CREATE TABLE IF NOT EXISTS members (
  customer_id VARCHAR(1),
  join_date DATE
);

INSERT INTO members
  VALUES
  ('A', '2021-01-07'),
  ('B', '2021-01-09');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Case Study Questions
-- MAGIC
-- MAGIC Each of the following case study questions can be answered using a single SQL statement:
-- MAGIC
-- MAGIC 1. What is the total amount each customer spent at the restaurant?
-- MAGIC 2. How many days has each customer visited the restaurant?
-- MAGIC 3. What was the first item from the menu purchased by each customer?
-- MAGIC 4. What is the most purchased item on the menu and how many times was it purchased by all customers?
-- MAGIC 5. Which item was the most popular for each customer?
-- MAGIC 6. Which item was purchased first by the customer after they became a member?
-- MAGIC 7. Which item was purchased just before the customer became a member?
-- MAGIC 8. What is the total items and amount spent for each member before they became a member?
-- MAGIC 9. If each $1 spent equates to 10 points and sushi has a 2x points multiplier - how many points would each customer have?
-- MAGIC 10. In the first week after a customer joins the program (including their join date) they earn 2x points on all items, not just sushi - how many points do customer A and B have at the end of January?

-- COMMAND ----------

-- Q1. What is the total amount each customer spent at the restaurant?

SELECT
  s.customer_id,
  SUM(m.price) as total_amount_spend
FROM dannys_diner.sales s
LEFT JOIN dannys_diner.menu m
ON s.product_id = m.product_id
GROUP BY s.customer_id
ORDER BY s.customer_id;

-- COMMAND ----------


--Q2. How many days has each customer visited the restaurant?

SELECT
  customer_id,
  count(DISTINCT order_date) as days_visted
FROM dannys_diner.sales
GROUP BY customer_id
ORDER BY days_visted DESC;

-- COMMAND ----------


--Q3. What was the first item from the menu purchased by each customer?
-- Note instead of order date if we had order datetime this would be more accurate

WITH ranked_sales AS (
  SELECT 
    s.customer_id,
    m.product_name,
    row_number() OVER (PARTITION BY s.customer_id ORDER BY s.order_date) AS rwnum
  FROM 
    dannys_diner.sales s
  LEFT JOIN 
    dannys_diner.menu m
  ON 
    s.product_id = m.product_id
)

SELECT
  customer_id,
  product_name
FROM ranked_sales
WHERE rwnum = 1;

-- COMMAND ----------


-- Q4. What is the most purchased item on the menu and how many times was it purchased by all customers?

SELECT 
  m.product_name,
  COUNT(s.product_id) AS total_purchases
FROM 
  sales s
INNER JOIN 
  menu m
ON 
  s.product_id = m.product_id
GROUP BY 
  1
ORDER BY 
  total_purchases DESC
LIMIT 1;

-- COMMAND ----------


--Q5. Which item was the most popular for each customer?

SELECT
  customer_id,
  product_name,
  count_of_order
FROM (
  SELECT
    customer_id,
    product_name,
    count_of_order,
    row_number() OVER (PARTITION BY customer_id ORDER BY customer_id) AS rnum
  FROM (
    SELECT
      customer_id,
      product_name,
      count_of_order,
      RANK() OVER (PARTITION BY customer_id ORDER BY count_of_order DESC) AS rnk
    FROM (
      SELECT
        customer_id,
        product_name,
        MAX(Count_of_orders) OVER (PARTITION BY customer_id ORDER BY product_id) AS count_of_order
      FROM (
        SELECT
          s.customer_id,
          s.product_id,
          m.product_name,
          COUNT(s.product_id) AS Count_of_orders
        FROM 
          sales s
        LEFT JOIN 
          menu m
        ON 
          s.product_id = m.product_id
        GROUP BY 
          1, 2, 3
      )
      GROUP BY 
        customer_id, product_name, product_id, Count_of_orders
      ORDER BY 
        customer_id, count_of_order DESC
    )
  )
  WHERE rnk = 1
)
WHERE rnum = 1;

-- COMMAND ----------


 -- Q6. Which item was purchased first by the customer after they became a member?
SELECT
  customer_id,
  product_name as first_thing_oredered_after_becoming_member
FROM (
        SELECT
          customer_id,
          product_name,
          row_number() OVER (PARTITION BY customer_id ORDER BY order_date) as rnum
        FROM (
                SELECT
                  j.customer_id,
                  j.order_date,
                  j.product_name,
                  mb.join_date
                FROM (
                  SELECT
                    s.customer_id,
                    s.order_date,
                    s.product_id,
                    m.product_name
                  FROM sales s
                  LEFT JOIN menu m
                  ON s.product_id = m.product_id
            ) j
            LEFT JOIN members mb
            ON j.customer_id = mb.customer_id
            WHERE 
              mb.join_date is NOT NULL
              AND j.order_date >= mb.join_date
        )
)
WHERE rnum = 1;

-- COMMAND ----------


--Q7. Which item was purchased just before the customer became a member?

SELECT
  customer_id,
  product_name
FROM (
      SELECT
        customer_id,
        product_name,
        order_date,
        row_number() OVER (PARTITION BY customer_id ORDER BY order_date DESC) as rnum
      FROM (
              SELECT
                j.customer_id,
                j.order_date,
                j.product_name,
                mb.join_date
              FROM (
                SELECT
                  s.customer_id,
                  s.order_date,
                  s.product_id,
                  m.product_name
                FROM sales s
                LEFT JOIN menu m
                ON s.product_id = m.product_id
          ) j
          LEFT JOIN members mb
          ON j.customer_id = mb.customer_id
          WHERE 
            mb.join_date is NOT NULL
            AND j.order_date < mb.join_date
      )
)
WHERE rnum = 1;

-- COMMAND ----------


--Q8 What is the total items and amount spent for each member before they became a member?

SELECT
customer_id,
count(product_name) as total_items,
sum(price) as total_amount_spent
FROM (
  SELECT
    j.customer_id,
    j.order_date,
    j.product_name,
    j.price,
    mb.join_date
  FROM (
    SELECT
      s.customer_id,
      s.order_date,
      s.product_id,
      m.product_name,
      m.price
    FROM sales s
    LEFT JOIN menu m
    ON s.product_id = m.product_id
  ) j
  LEFT JOIN members mb
  ON j.customer_id = mb.customer_id
  WHERE j.order_date < mb.join_date
)
GROUP BY customer_id
ORDER BY customer_id;

-- COMMAND ----------


--Q9. If each $1 spent equates to 10 points and sushi has a 2x points multiplier - how many points would each customer have?

SELECT
  customer_id,
  sum(price * points_of_one_dollar * multiplier) as total_points
FROM (
  SELECT
    s.customer_id,
    s.product_id,
    s.order_date,
    m.product_name,
    m.price,
    10 as points_of_one_dollar,
    (Case when m.product_name = 'sushi' then 2 else 1 end) as multiplier
  FROM sales s
  LEFT JOIN menu m
  ON s.product_id = m.product_id
)
GROUP BY customer_id
ORDER BY total_points DESC;

-- COMMAND ----------


--Q10. In the first week after a customer joins the program (including their join date) they earn 2x points on all items, not just sushi - how many points do customer A and B have at the end of January?
SELECT
  customer_id,
  month,
  sum(price * points_of_one_dollar * multiplier) as total_points
FROM (
    SELECT
      bt.customer_id,
      bt.product_id,
      bt.order_date,
      bt.product_name,
      bt.price,
      bt.points_of_one_dollar,
      (case 
          when 
              bt.order_date >= mb.join_date 
              AND
              bt.order_date <= date_add(mb.join_date, 7) 
          Then 2 
          else bt.multiplier 
      end) as multiplier,
      mb.join_date,
      date_format(bt.order_date, 'MMMM') as month
    FROM (
      SELECT
        s.customer_id,
        s.product_id,
        s.order_date,
        m.product_name,
        m.price,
        10 as points_of_one_dollar,
        (Case when m.product_name = 'sushi' then 2 else 1 end) as multiplier
      FROM sales s
      LEFT JOIN menu m
      ON s.product_id = m.product_id
    ) bt
    LEFT JOIN members mb
    ON bt.customer_id = mb.customer_id
)
WHERE join_date is not null and month = 'January'
GROUP BY customer_id,month
ORDER BY customer_id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Bonus Questions
-- MAGIC
-- MAGIC #### Join All The Things
-- MAGIC
-- MAGIC The following questions are related creating basic data tables that Danny and his team can use to quickly derive insights without needing to join the underlying tables using SQL.
-- MAGIC
-- MAGIC Recreate the following table output using the available data:
-- MAGIC
-- MAGIC | customer_id | order_date | product_name | price | member |
-- MAGIC |-------------|------------|--------------|-------|--------|
-- MAGIC | A           | 2021-01-01 | curry        | 15    | N      |
-- MAGIC | A           | 2021-01-01 | sushi        | 10    | N      |
-- MAGIC | A           | 2021-01-07 | curry        | 15    | Y      |
-- MAGIC | A           | 2021-01-10 | ramen        | 12    | Y      |
-- MAGIC | A           | 2021-01-11 | ramen        | 12    | Y      |
-- MAGIC | A           | 2021-01-11 | ramen        | 12    | Y      |
-- MAGIC | B           | 2021-01-01 | curry        | 15    | N      |
-- MAGIC | B           | 2021-01-02 | curry        | 15    | N      |
-- MAGIC | B           | 2021-01-04 | sushi        | 10    | N      |
-- MAGIC | B           | 2021-01-11 | sushi        | 10    | Y      |
-- MAGIC | B           | 2021-01-16 | ramen        | 12    | Y      |
-- MAGIC | B           | 2021-02-01 | ramen        | 12    | Y      |
-- MAGIC | C           | 2021-01-01 | ramen        | 12    | N      |
-- MAGIC | C           | 2021-01-01 | ramen        | 12    | N      |
-- MAGIC | C           | 2021-01-07 | ramen        | 12    | N      |

-- COMMAND ----------



SELECT
customer_id,
order_date,
product_name,
price,
(Case 
  when 
    order_date >= join_date 
    and 
    order_date < date_add(join_date,365 )
  THEN
    'Y'
  ELSE
    'N'
END
) member
FROM (
  SELECT
    j.customer_id,
    j.order_date,
    j.price,
    j.product_name,
    mb.join_date
  FROM (
    SELECT
      s.customer_id,
      s.order_date,
      m.product_name,
      m.price
    FROM sales s
    LEFT JOIN menu m
    ON s.product_id = m.product_id
    ORDER BY s.customer_id,s.order_date
  ) j
  LEFT JOIN members mb
  ON j.customer_id = mb.customer_id
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Rank All The Things
-- MAGIC
-- MAGIC Danny also requires further information about the ***ranking*** of customer products, but he purposely does not need the ***ranking*** for non-member purchases so he expects null ranking values for the records when customers are not yet part of the loyalty program.
-- MAGIC
-- MAGIC | customer_id | order_date | product_name | price | member | ranking |
-- MAGIC |-------------|------------|--------------|-------|--------|---------|
-- MAGIC | A           | 2021-01-01 | curry        | 15    | N      |         |
-- MAGIC | A           | 2021-01-01 | sushi        | 10    | N      |         |
-- MAGIC | A           | 2021-01-07 | curry        | 15    | Y      | 1       |
-- MAGIC | A           | 2021-01-10 | ramen        | 12    | Y      | 2       |
-- MAGIC | A           | 2021-01-11 | ramen        | 12    | Y      | 3       |
-- MAGIC | A           | 2021-01-11 | ramen        | 12    | Y      | 3       |
-- MAGIC | B           | 2021-01-01 | curry        | 15    | N      |         |
-- MAGIC | B           | 2021-01-02 | curry        | 15    | N      |         |
-- MAGIC | B           | 2021-01-04 | sushi        | 10    | N      |         |
-- MAGIC | B           | 2021-01-11 | sushi        | 10    | Y      | 1       |
-- MAGIC | B           | 2021-01-16 | ramen        | 12    | Y      | 2       |
-- MAGIC | B           | 2021-02-01 | ramen        | 12    | Y      | 3       |
-- MAGIC | C           | 2021-01-01 | ramen        | 12    | N      |         |
-- MAGIC | C           | 2021-01-01 | ramen        | 12    | N      |         |
-- MAGIC | C           | 2021-01-07 | ramen        | 12    | N      |         |

-- COMMAND ----------


CREATE TABLE IF NOT EXISTS base_table(
    SELECT
    customer_id,
    order_date,
    product_name,
    price,
    (Case 
      when 
        order_date >= join_date 
        and 
        order_date < date_add(join_date,365 )
      THEN
        'Y'
      ELSE
        'N'
    END
    ) member
    FROM (
      SELECT
        j.customer_id,
        j.order_date,
        j.price,
        j.product_name,
        mb.join_date
      FROM (
        SELECT
          s.customer_id,
          s.order_date,
          m.product_name,
          m.price
        FROM sales s
        LEFT JOIN menu m
        ON s.product_id = m.product_id
        ORDER BY s.customer_id,s.order_date
      ) j
      LEFT JOIN members mb
      ON j.customer_id = mb.customer_id
    )
);



With member_Y AS (
      SELECT
        customer_id,
        order_date,
        product_name,
        price,
        member,
        rank() OVER (PARTITION BY customer_id ORDER BY order_date) as ranking
      FROM base_table
      WHERE member = 'Y'
)

SELECT 
  customer_id,
  order_date,
  product_name,
  price,
  member,
  ranking 
FROM (
    (SELECT
      customer_id,
      order_date,
      product_name,
      price,
      member,
      Null as ranking
    FROM base_table
    WHERE member = 'N')
    UNION all
    (SELECT 
      customer_id,
      order_date,
      product_name,
      price,
      member,
      ranking
    FROM member_Y)
)
ORDER BY customer_id,order_date;
