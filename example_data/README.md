# Example input data

This directory will contain example input data for the database in the same format and structure as the production data should have.

The example data is based on HSL bus routes `1059`, `1500` and `2510` from 21.9.2020-27.11.2020 (Mon-Fri).

## Network links and nodes

`node.csv` and `link.csv` contain network data for `nw.node` and `nw.link` tables.
They include links and corresponding nodes used by the example bus routes.
The data is from [Digiroad](https://vayla.fi/vaylista/aineistot/digiroad/aineisto/rajapinnat), April 2021, and has been transformed for the database with [sujuikoNwPrepare](https://github.com/datarttu/sujuikoNwPrepare) R scripts and QGIS.

While PostGIS `geometry` fields would need binary (EWKB) input, geometries are stored as WKT in these files so they are better readable by human eye.
This also provides some better interoperability between different GIS tools.
Importing WKT geometries to the database is done with views `nw.view_node_wkt` and `nw.view_link_wkt` and `INSTEAD OF INSERT` triggers.
In a way, these views work as an API for data imports but for eye-checking validity of the existing geometries as well.

## Stops

*TO DO*

## Route versions

*TO DO*

## HFP observations

*TO DO*
