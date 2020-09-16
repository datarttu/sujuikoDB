# Rules for the network model

**TODO:** This rule model is aspirational: implement the network model later according to this.

## Global variables

`hsl_bbox`.
Bounding box of the HSL area, used to check that geometries lie approximately in a correct location.
The most obvious case is to avoid swapped X/Y or lon/lat coordinate errors.

`snap_tolerance` in projection units, in our case (TM35) meters.
Geometries closer to each other than `snap tolerance` are considered the same geometry.
This is to avoid cases where new geometries are created very close to each other although they should really be the same geometry but this cannot be easily seen visually on a map.
A reasonable value could be between 0.5 and 2 meters.

## Nodes

_Nodes_ represent separation points on street and track networks: intersections, link ends, and locations where two links with different attribute values must be separated.

### Table rules

The location of a node is represented by point geometry `geom`.
Multipoint nodes are not allowed.

A new node gets a _unique integer id_ `nodeid`.
Ids are given automatically in increasing order (data type `serial`), starting from 1.

If a node is deleted, its `nodeid` shall not be re-used.

### Trigger rules in order of execution

1. A node shall be located inside `hsl_bbox`.
*(`t01_validate_node_is_within_hsl_area`)*

1. Duplicated nodes are not allowed.
A node is duplicated if it is located closer than `snap_tolerance` to any existing node.
*(`t02_validate_node_unique_location`)*

1. If a node with links attached to it is moved, the link geometries are stretched and rotated such that they still connect to the node.
*(`t11_update_attached_link_geoms`)*

### Other rules

A node shall contain the following information about links connected to it:
number of outgoing one-way links (node is their `inode`),
number of incoming one-way links (node is their `jnode`),
number of connected two-way links (node is their `inode` and `jnode` of their reverses or vice versa).
These must be updated whenever a link is connected to or detached from a node, a connected link is changed from one-way to two-way or vice versa, or the geometry of a connected one-way link is reversed.
**TODO: view based on link inode and jnode**

## Links

_Links_ represent connections between nodes.

### Table rules

The location and form of a link is represented by linestring geometry `geom`.
Multilinestring links are not allowed.

A new link gets a _unique integer id_ `linkid`.
Ids are given automatically in increasing order (data type `serial`), starting from 1.

A link is either one- or two-way.
One-way links can only be traversed from `inode` to `jnode`.
Two-way links can be traversed from `inode` to `jnode` and from `jnode` to `inode`.
*Indicated by boolean `oneway` field.*

A link has a transit `mode` that is allowed to use the link: `bus` and / or `tram`.

A link has a `cost` equal to its geometry length in projection units, and similarly, reverse cost `rcost`.
For two-way links, `rcost == cost`.
For one-way links, `rcost == -1`.
When link geometry is modified, the costs must be updated accordingly.
*`cost` is a generated column based on link geometry length, and `rcost` is a generated column based on link geometry length and `oneway` value.*

### Trigger rules in order of execution

1. The geometries of a new / modified and an existing link may _cross_ and their ends may _touch_ each other.
Their geometries shall neither _intersect_ nor _touch_ in other ways.
*(`t01_validate_geom_relationships`)*
1. If a link is created or its geometry is updated and there is an existing node at the link start, that node is defined as the `inode` of the link.
**TODO:** *(`t06_update_inode_reference`)*
1. If a link is created or its geometry is updated and there is an existing node at the link end, that node is defined as the `jnode` of the link.
**TODO:** *(`t07_update_jnode_reference`)*
1. If the start of a new or modified link geometry does not lie exactly at an existing node but within less than `snap_tolerance` from it, the link geometry is stretched and rotated such that the start touches the node.
*(`t11_snap_geom_to_inode`)*
1. If the end of a new or modified link geometry does not lie exactly at an existing node but within less than `snap_tolerance` from it, the link geometry is stretched and rotated such that the end touches the node.
*(`t12_snap_geom_to_jnode`)*
1. If a new / modified link does not have an existing `inode` at its start, a new node is created.
**TODO:** *(`t21_add_missing_inode`)*
1. If a new / modified link does not have an existing `jnode` at its end, a new node is created.
**TODO:** *(`t22_add_missing_jnode`)*
1. A link shall have a start node `inode`, an existing node that is located exactly on the first vertex of the link geometry.
Similarly, it shall have an end node `jnode`, an existing node that is located exactly on the last vertex of the link geometry.
*(`t31_validate_link_node_references`)*
1. `inode` and `jnode` of a link shall not be the same node.
*(`t32_validate_link_inode_jnode_not_eq`)*

### Other rules

A link geometry shall not be shorter than `snap_tolerance`.
*This is guaranteed by the rule that two nodes may not be closer than `snap_tolerance`.*

The reverse representation of a two-way link is generated by negating the `linkid`, e.g., `1234 -> -1234`.
In that case, the geometry of the original link is reversed and `inode` and `jnode` are flipped.
**TODO:** *materialized view with reverse links where linkids are negated.*

Splitting a link is equal to modifying the `inode` or `jnode`, geometry and costs of the original link into "part 1" and creating a new link as "part 2".

Merging two links "link 1" and "link 2" is equal to setting link 1's `inode`/`jnode` to link 2's `inode`/`jnode`, link 1's geometry to the union of the geometries of the both links, link 1's cost to the sum of the two links' costs, and deleting link 2.

## Stops

**TODO**

## Intersections

**TODO**
