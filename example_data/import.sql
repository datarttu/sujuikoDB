\copy nw.view_node_wkt FROM '/data/node.csv' CSV HEADER;
\copy nw.view_link_wkt FROM '/data/link.csv' CSV HEADER;
\copy nw.view_stop_wkt FROM '/data/stop.csv' CSV HEADER;
\copy nw.route_version (route_ver_id, route, dir, valid_during, route_mode) FROM '/data/route_version.csv' CSV HEADER;
\copy nw.stop_on_route (route_ver_id, stop_seq, stop_id, active_place) FROM '/data/stop_on_route.csv' CSV HEADER;
CALL nw.update_stop_link_refs(20.0);

\copy nw.section (section_id, description, via_nodes) FROM '/data/section.csv' CSV HEADER;
CALL nw.batch_upsert_links_on_section();
CALL nw.upsert_links_on_section('akk_sture_teoll_2', ARRAY[240, 214]);
CALL nw.upsert_links_on_section('non_existing_section', ARRAY[240, 214]);

CALL nw.batch_upsert_links_on_route();
INSERT INTO nw.manual_vianode_on_route (route_ver_id, after_stop_seq, sub_seq, node_id)
  VALUES ('1059_1_20200921_20201015', 25, 1, 2);
CALL nw.upsert_links_on_route(target_route_ver_id := '1059_1_20200921_20201015');

-- Custom append_unique() in 010_global.sql
SELECT append_unique(array['foo', 'bar'], 'baz') = array['foo', 'bar', 'baz'];
SELECT append_unique(array['foo', 'bar'], 'bar') = array['foo', 'bar'];

CALL nw.validate_nodes();
CALL nw.validate_links();
CALL nw.validate_stops();
CALL nw.validate_stops_on_route();

\copy obs.journey(jrnid, route, dir, start_tst, oper, veh) FROM '/data/hfp/jrn_2510_1_2020-09-23.csv' CSV HEADER;
