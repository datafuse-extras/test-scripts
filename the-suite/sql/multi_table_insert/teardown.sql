(SELECT 't0' as table_name, count(*) FROM fuse_snapshot('default', 't0'))
UNION ALL
(SELECT 't1' as table_name, count(*) FROM fuse_snapshot('default', 't1'))
UNION ALL
(SELECT 't2' as table_name, count(*) FROM fuse_snapshot('default', 't2'))
UNION ALL
(SELECT 't3' as table_name, count(*) FROM fuse_snapshot('default', 't3'))
UNION ALL
(SELECT 't4' as table_name, count(*) FROM fuse_snapshot('default', 't4'))
UNION ALL
(SELECT 't5' as table_name, count(*) FROM fuse_snapshot('default', 't5'))
UNION ALL
(SELECT 't6' as table_name, count(*) FROM fuse_snapshot('default', 't6'))
UNION ALL
(SELECT 't7' as table_name, count(*) FROM fuse_snapshot('default', 't7'))
UNION ALL
(SELECT 't8' as table_name, count(*) FROM fuse_snapshot('default', 't8'))
UNION ALL
(SELECT 't9' as table_name, count(*) FROM fuse_snapshot('default', 't9'));
