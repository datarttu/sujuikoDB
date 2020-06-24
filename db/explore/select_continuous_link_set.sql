BEGIN;
CREATE VIEW links_extra AS (
  WITH
    nodes_o AS (
      SELECT
        inode         AS node,
        count(linkid) AS outgoing
      FROM nw.links
      GROUP BY node
    ),
    nodes_i AS (
      SELECT
        jnode         AS node,
        count(linkid) AS incoming
      FROM nw.links
      GROUP BY node
    ),
    nodes_io AS (
      SELECT
        o.node,
        coalesce(o.outgoing, 0) AS outgoing,
        coalesce(i.incoming, 0) AS incoming
      FROM nodes_o      AS o
      FULL JOIN nodes_i AS i
        ON o.node = i.node
    )
  SELECT
    l.linkid, l.inode, l.jnode,
    n_i.outgoing AS i_out,
    n_i.incoming AS i_in,
    n_j.outgoing AS j_out,
    n_j.incoming AS j_in
  FROM nw.links       AS l
  INNER JOIN nodes_io AS n_i
    ON l.inode = n_i.node
  INNER JOIN nodes_io AS n_j
    ON l.jnode = n_j.node
);

WITH RECURSIVE links_path AS (
  SELECT linkid, inode, jnode, i_out, i_in, j_out, j_in
  FROM links_extra
  WHERE linkid = 1236
  UNION
  SELECT l.linkid, l.inode, l.jnode, l.i_out, l.i_in, l.j_out, l.j_in
  FROM links_extra      AS l
  INNER JOIN links_path AS lp
    ON
      -- Continuous links BEFORE the root link
      (l.jnode = lp.inode AND l.j_out = 1 AND l.j_in = 1)
      OR
      -- Continuous links AFTER the root link
      (l.inode = lp.jnode AND l.i_in = 1 AND l.i_out = 1)
)
SELECT * FROM links_path;

ROLLBACK;
