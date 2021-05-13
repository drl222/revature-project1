create database if not exists project1;
use project1;
drop table bevbranch;
-- hdfs dfs -put /home/pokem/revature-project1/Bev_Branch*.txt /user/pokem/project1/Branch
CREATE TABLE BevBranch(beverage STRING, branch STRING)
	PARTITIONED BY (partition_id INT)
	ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
	STORED AS TEXTFILE;
LOAD DATA INPATH '/user/pokem/project1/Branch' INTO TABLE BevBranch PARTITION(partition_id = 1);

-- hdfs dfs -put /home/pokem/revature-project1/Bev_Conscount*.txt /user/pokem/project1/Conscount
CREATE TABLE BevConscount(beverage STRING, consumercount STRING)
	PARTITIONED BY (partition_id INT)
	ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
	STORED AS TEXTFILE;
LOAD DATA INPATH '/user/pokem/project1/Conscount' INTO TABLE BevConscount PARTITION(partition_id = 1);

SELECT * FROM BevBranch;
SELECT * FROM BevConscount;

-- Scenario 1
-- What is the total number of consumers for Branch1?
-- What is the number of consumers for the Branch2?
SELECT BB.Branch, sum(BC.consumercount) AS total_consumers
	FROM BevBranch BB INNER JOIN BevConscount BC ON BB.beverage = BC.beverage
	GROUP BY BB.Branch; -- Branch 1: 1115974, Branch 2: 5099141

-- Scenario 2
-- What is the most consumed beverage on Branch1
-- What is the least consumed beverage on Branch2
SELECT BC.beverage, sum(BC.consumercount) as total_consumers
	FROM BevBranch BB INNER JOIN BevConscount BC ON BB.beverage = BC.beverage
	WHERE BB.Branch ='Branch1'
	GROUP BY BC.beverage
	ORDER BY total_consumers DESC LIMIT 1; -- Special_cappuccino, 108163
SELECT BC.beverage, sum(BC.consumercount) as total_consumers
	FROM BevBranch BB INNER JOIN BevConscount BC ON BB.beverage = BC.beverage
	WHERE BB.Branch ='Branch2'
	GROUP BY BC.beverage
	ORDER BY total_consumers ASC LIMIT 1; -- Cold_MOCHA, 47524

-- Scenario 3
-- What are the beverages available on Branch10, Branch8, and Branch1?
-- what are the comman beverages available in Branch4,Branch7?
SELECT DISTINCT beverage FROM BevBranch BB WHERE branch = 'Branch10' OR branch = 'Branch8' OR branch = 'Branch1'; -- 44 results
SELECT beverage FROM BevBranch BB WHERE branch = 'Branch4' INTERSECT SELECT beverage FROM BevBranch BB WHERE branch = 'Branch7'; -- 51 results

-- Scenario 4
-- create a partition,index,View for the scenario3.
ALTER TABLE BevBranch ADD PARTITION (partition_id = 2);
CREATE INDEX bevbranch_on_branch_idx ON TABLE BevBranch(branch) AS 'org.apache.hadoop.hive.ql.index.compact.CompactIndexHandler' WITH DEFERRED REBUILD;
ALTER INDEX bevbranch_on_branch_idx ON BevBranch REBUILD;
CREATE VIEW scenario_3_part1_answer AS SELECT DISTINCT beverage FROM BevBranch BB WHERE branch = 'Branch10' OR branch = 'Branch8' OR branch = 'Branch1';
CREATE VIEW scenario_3_part2_answer AS SELECT beverage FROM BevBranch BB WHERE branch = 'Branch4' INTERSECT SELECT beverage FROM BevBranch BB WHERE branch = 'Branch7';

SELECT * FROM scenario_3_part1_answer;
SELECT * FROM scenario_3_part2_answer;

-- Scenario 5
-- Alter the table properties to add "note","comment"
ALTER TABLE BevConscount SET TBLPROPERTIES('note'='Each row counts some of the customers available for each beverage type.');
ALTER TABLE BevConscount SET TBLPROPERTIES('comment'='Remember that there are multiple rows that contain the same beverage!');
SHOW TBLPROPERTIES BevConscount;

-- Scenario 6
-- Remove the row 5 from the output of Scenario 1 

SELECT branch, total_consumers FROM (
	SELECT BB.Branch, sum(BC.consumercount) AS total_consumers, ROW_NUMBER() over () as row_num
	FROM BevBranch BB INNER JOIN BevConscount BC ON BB.beverage = BC.beverage
	GROUP BY BB.Branch
	ORDER BY BB.branch ASC
) S WHERE row_num != 5;