-- Insert all 10 records into Database1.Table1 with a single statement
INSERT INTO Database1.Table1 (id, name)
VALUES 
  (1, 'John Smith'),
  (2, 'Mary Johnson'),
  (3, 'Robert Brown'),
  (4, 'Patricia Davis'),
  (5, 'James Wilson'),
  (6, 'Linda Martinez'),
  (7, 'Michael Anderson'),
  (8, 'Elizabeth Thomas'),
  (9, 'William Jackson'),
  (10, 'Jennifer White');

-- Insert all 10 records into Database2.Table2 with a single statement
INSERT INTO Database2.Table2 (id, name)
VALUES 
  (1, 'John Smith'),
  (2, 'Mary Johnson'),
  (3, 'Robert Brown'),
  (4, 'Patricia Davis'),
  (5, 'James Wilson'),
  (11, 'Susan Harris'),
  (12, 'David Clark'),
  (13, 'Nancy Lewis'),
  (14, 'Joseph Walker'),
  (15, 'Karen Hall');


-- 1. INNER JOIN
-- Returns only matching rows from both tables
SELECT t1.id as t1_id, t1.name as t1_name, 
       t2.id as t2_id, t2.name as t2_name
FROM Database1.Table1 t1
INNER JOIN Database2.Table2 t2 ON t1.id = t2.id;

-- 2. LEFT OUTER JOIN
-- Returns all rows from left table and matching rows from right table
SELECT t1.id as t1_id, t1.name as t1_name, 
       t2.id as t2_id, t2.name as t2_name
FROM Database1.Table1 t1
LEFT OUTER JOIN Database2.Table2 t2 ON t1.id = t2.id;

-- 3. RIGHT OUTER JOIN
-- Returns all rows from right table and matching rows from left table
SELECT t1.id as t1_id, t1.name as t1_name, 
       t2.id as t2_id, t2.name as t2_name
FROM Database1.Table1 t1
RIGHT OUTER JOIN Database2.Table2 t2 ON t1.id = t2.id;

-- 4. FULL OUTER JOIN
-- Returns all rows from both tables, matching where possible
SELECT t1.id as t1_id, t1.name as t1_name, 
       t2.id as t2_id, t2.name as t2_name
FROM Database1.Table1 t1
FULL OUTER JOIN Database2.Table2 t2 ON t1.id = t2.id;
