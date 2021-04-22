\copy nw.view_node_wkt FROM '/data/node.csv' CSV HEADER;
\copy nw.view_link_wkt FROM '/data/link.csv' CSV HEADER;
\copy nw.view_stop_wkt FROM '/data/stop.csv' CSV HEADER;
\copy nw.route_version (route_ver_id, route, dir, valid_during, route_mode) FROM '/data/route_version.csv' CSV HEADER;
\copy nw.stop_on_route FROM '/data/stop_on_route.csv' CSV HEADER;
