/*
 * Copy of manual position corrections made to stops
 * in stage_gtfs.stops_with_mode
 * as of 2020-06-30.
 * Run this afterwards if importing stops from scratch.
 */

\set ON_ERROR_STOP on

BEGIN;

UPDATE stage_gtfs.stops_with_mode SET geom = '0101000020FB0B0000E67978DE7B981741DB34B3EAA1735941' WHERE stopid = 1010424;
UPDATE stage_gtfs.stops_with_mode SET geom = '0101000020FB0B00006F607A5FC88817415CE82A58C6735941' WHERE stopid = 1020443;
UPDATE stage_gtfs.stops_with_mode SET geom = '0101000020FB0B00009F4E3F1995931741D91E60109F735941' WHERE stopid = 1020450;
UPDATE stage_gtfs.stops_with_mode SET geom = '0101000020FB0B0000C4A1EC2608931741A4F78369A0735941' WHERE stopid = 1020455;
UPDATE stage_gtfs.stops_with_mode SET geom = '0101000020FB0B0000FDC609A212911741C419DAFBEA735941' WHERE stopid = 1020458;
UPDATE stage_gtfs.stops_with_mode SET geom = '0101000020FB0B0000A57BF269088A174118BA743372725941' WHERE stopid = 1050401;
UPDATE stage_gtfs.stops_with_mode SET geom = '0101000020FB0B0000AA4953205E8E17416701626876725941' WHERE stopid = 1070121;
UPDATE stage_gtfs.stops_with_mode SET geom = '0101000020FB0B000033D66FC1A58E1741D6F3AC3F75725941' WHERE stopid = 1070122;
UPDATE stage_gtfs.stops_with_mode SET geom = '0101000020FB0B0000E688E832608E1741B483673776725941' WHERE stopid = 1070421;
UPDATE stage_gtfs.stops_with_mode SET geom = '0101000020FB0B000019B68365A58E17414731893275725941' WHERE stopid = 1070422;
UPDATE stage_gtfs.stops_with_mode SET geom = '0101000020FB0B0000136FDDEE559D1741A89A774C6F735941' WHERE stopid = 1080404;
UPDATE stage_gtfs.stops_with_mode SET geom = '0101000020FB0B00005903C060E99F17416C11411C5A735941' WHERE stopid = 1080405;
UPDATE stage_gtfs.stops_with_mode SET geom = '0101000020FB0B00005EE17800ECA51741CAC9D9C156735941' WHERE stopid = 1080410;
UPDATE stage_gtfs.stops_with_mode SET geom = '0101000020FB0B0000B8E83525E6A21741A9F1E46B3A735941' WHERE stopid = 1080415;
UPDATE stage_gtfs.stops_with_mode SET geom = '0101000020FB0B0000F5DAC5CC5A981741062BBEB5CD725941' WHERE stopid = 1090416;
UPDATE stage_gtfs.stops_with_mode SET geom = '0101000020FB0B0000FBD9C46C90A91741A880658440755941' WHERE stopid = 1100130;
UPDATE stage_gtfs.stops_with_mode SET geom = '0101000020FB0B000069A1D3C92D961741DE9D879493755941' WHERE stopid = 1121406;
UPDATE stage_gtfs.stops_with_mode SET geom = '0101000020FB0B0000EE059417398317414D2433A8E1735941' WHERE stopid = 1130437;
UPDATE stage_gtfs.stops_with_mode SET geom = '0101000020FB0B00009A422A856A7B17410CAF72A275745941' WHERE stopid = 1130443;
UPDATE stage_gtfs.stops_with_mode SET geom = '0101000020FB0B0000CCD493A38779174164D9F43BD0755941' WHERE stopid = 1140440;
UPDATE stage_gtfs.stops_with_mode SET geom = '0101000020FB0B00002ECDC231D07C17417FAA2E41D1745941' WHERE stopid = 1140449;
UPDATE stage_gtfs.stops_with_mode SET geom = '0101000020FB0B00009A8DDED1A9801741522EB71512755941' WHERE stopid = 1140451;
UPDATE stage_gtfs.stops_with_mode SET geom = '0101000020FB0B000087C156128E8D17416041D5F0A6765941' WHERE stopid = 1173403;
UPDATE stage_gtfs.stops_with_mode SET geom = '0101000020FB0B000078964B1ECB7217419F9E5A022E765941' WHERE stopid = 1180437;
UPDATE stage_gtfs.stops_with_mode SET geom = '0101000020FB0B00001B366416D36E174126A1B0455D765941' WHERE stopid = 1180439;
UPDATE stage_gtfs.stops_with_mode SET geom = '0101000020FB0B000098F025FA596B1741EDC503A18B765941' WHERE stopid = 1180441;
UPDATE stage_gtfs.stops_with_mode SET geom = '0101000020FB0B0000B71052B4D4781741A5C55DEC15735941' WHERE stopid = 1201133;
UPDATE stage_gtfs.stops_with_mode SET geom = '0101000020FB0B000044CF9BE2D37817414EA7691016735941' WHERE stopid = 1201430;
UPDATE stage_gtfs.stops_with_mode SET geom = '0101000020FB0B0000172E1BDCF0791741A2B591C0AE725941' WHERE stopid = 1203112;
UPDATE stage_gtfs.stops_with_mode SET geom = '0101000020FB0B00009A13458CD7791741E5879596AE725941' WHERE stopid = 1203405;
UPDATE stage_gtfs.stops_with_mode SET geom = '0101000020FB0B00007EE454EEEC791741681E17BBAE725941' WHERE stopid = 1203406;
UPDATE stage_gtfs.stops_with_mode SET geom = '0101000020FB0B0000D054FBB4607417415EB8746994725941' WHERE stopid = 1203415;
UPDATE stage_gtfs.stops_with_mode SET geom = '0101000020FB0B0000739DCDC49D901741C96D399908775941' WHERE stopid = 1220426;
UPDATE stage_gtfs.stops_with_mode SET geom = '0101000020FB0B000013D866B8F98E1741B5080FE73F765941' WHERE stopid = 1220431;
UPDATE stage_gtfs.stops_with_mode SET geom = '0101000020FB0B0000F1A4F1E7E4A41741AEBC3D81D0775941' WHERE stopid = 1240403;
UPDATE stage_gtfs.stops_with_mode SET geom = '0101000020FB0B0000317F2D89E5A217410B17191C4B775941' WHERE stopid = 1240418;
UPDATE stage_gtfs.stops_with_mode SET geom = '0101000020FB0B00001CFC5AE2DD70174116305D73157A5941' WHERE stopid = 1281160;
UPDATE stage_gtfs.stops_with_mode SET geom = '0101000020FB0B00008E73F412AC5A1741C88B83D5C3765941' WHERE stopid = 1301450;
UPDATE stage_gtfs.stops_with_mode SET geom = '0101000020FB0B000020F2EDC3BE5517413D38D55CE6765941' WHERE stopid = 1301452;
UPDATE stage_gtfs.stops_with_mode SET geom = '0101000020FB0B000013CC1D35ED921741BCFBF49EBB7B5941' WHERE stopid = 1341104;
UPDATE stage_gtfs.stops_with_mode SET geom = '0101000020FB0B0000F606254F00001841FAE07B88D7795941' WHERE stopid = 1454113;
UPDATE stage_gtfs.stops_with_mode SET geom = '0101000020FB0B00002423288DE9EC17416068085DCB785941' WHERE stopid = 1454120;
UPDATE stage_gtfs.stops_with_mode SET geom = '0101000020FB0B00006E0C503A22E717414C599A7C75785941' WHERE stopid = 1454184;
UPDATE stage_gtfs.stops_with_mode SET geom = '0101000020FB0B0000B22261A5FAEA17412DE4B2DB73745941' WHERE stopid = 1491157;
UPDATE stage_gtfs.stops_with_mode SET geom = '0101000020FB0B0000F61F468149D017416F86DD357E735941' WHERE stopid = 1494104;
UPDATE stage_gtfs.stops_with_mode SET geom = '0101000020FB0B00000EE09891A56818412E74573D907C5941' WHERE stopid = 1601205;
UPDATE stage_gtfs.stops_with_mode SET geom = '0101000020FB0B00009B3EB0F1B02A174174D2DC4B7C745941' WHERE stopid = 2213220;
UPDATE stage_gtfs.stops_with_mode SET geom = '0101000020FB0B0000AF9DEF2705491741A81C5A0480895941' WHERE stopid = 4320215;
UPDATE stage_gtfs.stops_with_mode SET geom = '0101000020FB0B0000A78B75B8F6D81541D7C04340CE705941' WHERE stopid = 6040247;

COMMIT;
