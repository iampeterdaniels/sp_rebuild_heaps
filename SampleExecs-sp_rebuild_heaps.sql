-- Don't actually rebuild - just output the TSQL.  Look for scans in last 7 days
EXEC sp_rebuild_heaps @Execute = 0

-- Execute the rebuilds, Look for scans in last 7 days
EXEC sp_rebuild_heaps


