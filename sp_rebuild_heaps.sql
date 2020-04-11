IF OBJECT_ID('dbo.sp_rebuild_heaps') IS null BEGIN
	EXEC ('CREATE PROCEDURE dbo.sp_rebuild_heaps AS RETURN 138;');
END;
GO
-- Rebuild heaps that have had user scans in the last @ScannedWithinLastNumberOfDays days 
-- and have non-zero forwarded fetch counts.
ALTER PROCEDURE [dbo].[sp_rebuild_heaps] (
	@Execute BIT = 1
	,@Database sysname = N'all'
	,@ScannedWithinLastNumberOfDays SMALLINT = 7
)
WITH RECOMPILE
AS
BEGIN

SET NOCOUNT, XACT_ABORT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET ANSI_WARNINGS OFF;

-- Internal variables
DECLARE @DBID INT
	,@MaxDBID INT
	,@database_id INT
	,@DBName sysname
	,@TableName sysname
	,@SchemaName sysname
	,@TableID INT
    ,@MaxTableID INT
	,@ForwardedFetchCount BIGINT
	,@SQL VARCHAR(500)
	,@FullyQualifiedObjectName VARCHAR(500)

-- Temp Tables & vars
DECLARE @DB TABLE (DBID INT IDENTITY NOT NULL PRIMARY KEY, database_id INT NOT NULL, DBName sysname NOT NULL, MaxLastUserScanOnHeap DATETIME2 NULL)
CREATE TABLE #Table (TableID INT IDENTITY NOT NULL PRIMARY KEY, SchemaName sysname NOT NULL, TableName sysname NOT NULL, ForwardedFetchCount BIGINT NOT NULL)

-- Log a header
PRINT '-- **************************************************************************************************'
PRINT '-- **************************************************************************************************'
PRINT '-- Rebuild Active Heaps - starting at: ' + CONVERT(VARCHAR(50), SYSDATETIME())

-- Process input DB?  Or all DBs?
IF @Database = 'all' BEGIN 
	-- Find the DBs that have heap scans in the last 7 days, order by most recently scanned DBs
	INSERT INTO @DB (database_id, DBName, MaxLastUserScanOnHeap)
	SELECT
		ddius.database_id
		,DB_NAME(ddius.database_id) AS DBName
		,MAX(ddius.last_user_scan) AS MaxLastUserScanOnHeap
	FROM
		sys.dm_db_index_usage_stats AS ddius
	WHERE
		ddius.database_id > 4
		AND ddius.index_id = 0 -- Heap
	GROUP BY
		ddius.database_id
		,DB_NAME(ddius.database_id)
	HAVING
		MAX(ddius.last_user_scan) > DATEADD(DAY, -7, SYSDATETIME())
	ORDER BY
		MAX(ddius.last_user_scan) DESC
END ELSE BEGIN
	INSERT INTO @DB (database_id, DBName, MaxLastUserScanOnHeap)
	SELECT
		ddius.database_id
		,DB_NAME(ddius.database_id) AS DBName
		,MAX(ddius.last_user_scan) AS MaxLastUserScanOnHeap
	FROM
		sys.dm_db_index_usage_stats AS ddius
	WHERE
		ddius.database_id  = DB_ID(@Database)
		AND ddius.index_id = 0 -- Heap
	GROUP BY
		ddius.database_id
		,DB_NAME(ddius.database_id)
	HAVING
		MAX(ddius.last_user_scan) > DATEADD(DAY, -7, SYSDATETIME())
	ORDER BY
		MAX(ddius.last_user_scan) DESC
END

SELECT @DBID = 1, @MaxDBID = COALESCE(MAX(d.DBID), 0) FROM @DB AS d;

IF @MaxDBID = 0 BEGIN
	PRINT 'No databases meet the criteria.  Exiting.';
	RETURN 0;
END;

-- We have some DBs that meet our criteria - let's loop through them.
WHILE @DBID <= @MaxDBID BEGIN
	SELECT
		@database_id = d.database_id
		,@DBName = d.DBName
	FROM
		@DB AS d
	WHERE
		d.DBID = @DBID

	PRINT '-- Rebuilding active heaps on ' + @DBName + REPLICATE('*', (50 - LEN(@DBName)))

	-- First, clean out our table list to start fresh.
	TRUNCATE TABLE #Table;

	-- Get the heaps scanned in the last 7 days that have forward fetch counts > 0
	INSERT #Table
	(
		SchemaName
	    ,TableName
		,ForwardedFetchCount
	)
	SELECT
		OBJECT_SCHEMA_NAME(ddius.object_id, ddius.database_id) AS SchemaName
		,OBJECT_NAME(ddius.object_id, ddius.database_id) AS TableName
		--,ddius.user_scans
		--,ddius.last_user_scan
		--,ddios.range_scan_count
		,ddios.forwarded_fetch_count
		--,'ALTER TABLE dbo.' + OBJECT_NAME(ddius.object_id, ddius.database_id) + ' REBUILD;' AS SQLStmt
	FROM
		sys.dm_db_index_usage_stats AS ddius
		CROSS APPLY sys.dm_db_index_operational_stats(ddius.database_id, ddius.object_id, 0, DEFAULT) AS ddios
	WHERE
		ddius.database_id = @database_id 
		AND ddius.index_id = 0
		-- Let's narrow this down to ones that have user scans in the last week 
		AND ddius.last_user_scan > DATEADD(DAY, -7, SYSDATETIME())
		-- and non-zero forwarded fetch count
		AND ddios.forwarded_fetch_count > 0
	ORDER BY
		ddios.forwarded_fetch_count DESC

	-- Loop on each table
	SELECT
		@TableID = 1
		,@MaxTableID = MAX(TableID)
	FROM
		#Table

	WHILE @TableID <= @MaxTableID BEGIN
		SELECT
			@SchemaName = t.SchemaName
			,@TableName = t.TableName
			,@ForwardedFetchCount = t.ForwardedFetchCount
		FROM
			#Table AS t
		WHERE
			t.TableID = @TableID

		-- Build and execute the SQL to rebuild the heap (and all NCs)
		SET @FullyQualifiedObjectName = '[' + @DBName + '].[' + @SchemaName + '].[' + @TableName + ']'
		PRINT '-- Rebuilding ' + @FullyQualifiedObjectName + ', ForwardFetchCount = ' + CONVERT(VARCHAR(50), @ForwardedFetchCount)
		SET @SQL = 'ALTER TABLE ' + @FullyQualifiedObjectName + ' REBUILD;'
		IF @Execute = 1 BEGIN
			EXEC(@SQL)
		END ELSE BEGIN
			PRINT @SQL
		END
		PRINT ''

		SET @TableID = @TableID + 1
	END

	PRINT ''
	SET @DBID = @DBID + 1
END

END
GO