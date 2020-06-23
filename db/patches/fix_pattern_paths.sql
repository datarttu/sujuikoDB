/*
 * Some manual fixes to incorrect records in stage_gtfs.pattern_paths,
 * pretty much just for an example.
 */

BEGIN;

-- 1018 turn loop in Tali
DELETE FROM stage_gtfs.stop_pair_paths
WHERE inode = 800 AND jnode = 583;
INSERT INTO stage_gtfs.stop_pair_paths (
  inode, jnode, path_seq, nodeid, linkid
) VALUES
(800, 583, 1, 800, 518),
(800, 583, 2, 801, 816),
(800, 583, 3, 1319, 11637),
(800, 583, 4, 801, 518),
(800, 583, 5, 800, 517),
(800, 583, 6, 584, 267),
(800, 583, 7, 583, -1);

DELETE FROM stage_gtfs.stop_pair_paths
WHERE inode = 800 AND jnode = 6414;
INSERT INTO stage_gtfs.stop_pair_paths (
  inode, jnode, path_seq, nodeid, linkid
) VALUES
(800, 6414, 1, 800, 518),
(800, 6414, 2, 801, 816),
(800, 6414, 3, 1319, 11637),
(800, 6414, 4, 801, 518),
(800, 6414, 5, 800, 517),
(800, 6414, 6, 584, 5394),
(800, 6414, 7, 6414, -1);

COMMIT;
